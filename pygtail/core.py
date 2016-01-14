#!/usr/bin/python -tt
# -*- coding: utf-8 -*-

# pygtail - a python "port" of logtail2
# Copyright (C) 2011 Brad Greenlee <brad@footle.org>
#
# Derived from logcheck <http://logcheck.org>
# Copyright (C) 2003 Jonathan Middleton <jjm@ixtab.org.uk>
# Copyright (C) 2001 Paul Slootman <paul@debian.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

from __future__ import print_function
from os import stat, fstat
from os.path import exists, getsize
from datetime import datetime, timedelta
import gzip
import logging
import signal
import socket
import sys
import time
from optparse import OptionParser

__version__ = '0.5.3'

PY3 = sys.version_info[0] == 3
if PY3:
    text_type = str
else:
    text_type = unicode


def force_text(s, encoding='utf-8', errors='strict'):
    if isinstance(s, text_type):
        return s
    return s.decode(encoding, errors)


class Pygtail(object):
    """
    Creates an iterable object that returns only unread lines.

    Keyword arguments:
    offset_file   File to which offset data is written (default: <logfile>.offset).
    paranoid      Update the offset file every time we read a line (as opposed to
                  only when we reach the end of the file (default: False)
    copytruncate  Support copytruncate-style log rotation (default: True)
    """
    def __init__(self, filename, offset_file=None, paranoid=False,
                 copytruncate=True, wait_step=0.5, wait_timeout=20,
                 host_name=None):
        self.filename = filename
        self.paranoid = paranoid
        self.copytruncate = copytruncate
        self.wait_step = wait_step
        self.wait_timeout = wait_timeout
        self.time_waited = 0.0
        self._offset_file = offset_file or "%s.offset" % self.filename
        self._offset_file_inode = None
        self._offset = None
        self._dt_format = '%Y-%m-%dT%H:%M:%S.%f'
        self._hostname = host_name or socket.gethostname().split('.')[0]
        self._filename_format = '%(filename)s_%(host_name)s_%(log_hour)s.gz'
        self._log_hour_format = '%Y%m%d%H'
        self._fh = None
        self._rotated_logfiles = []
        self._catching_up = False
        self._last_log = None
        self._parse_offset_file()
        if self._last_log:
            self._rotated_logfiles = self._determine_rotated_logfiles()
            self._catching_up = bool(self._rotated_logfiles)

    def __del__(self):
        self._update_offset_file()
        if not self._is_closed():
            self._fh.close()

    def __iter__(self):
        return self

    def next(self):
        """
        Return the next line in the file, updating the offset.
        """
        try:
            line = self._get_next_line()
        except StopIteration:
            if self._catching_up:
                logging.debug(
                    "Finished processing %s, moving to %s",
                    getattr(self._fh, 'filename') or getattr(self._fh, 'name'),
                    self._rotated_logfiles and self._rotated_logfiles[0] or self.filename
                )
                self._reload()
                self._catching_up = bool(self._rotated_logfiles)
                # Start on the next rotated file
                try:
                    line = self._get_next_line()
                except StopIteration:  # oops, empty file
                    self._update_offset_file()
                    raise
            else:
                logging.debug("StopIteration at the main file, exiting")
                self._update_offset_file()
                raise

        if self.paranoid:
            self._update_offset_file()

        return line

    def __next__(self):
        """`__next__` is the Python 3 version of `next`"""
        return self.next()

    def readlines(self):
        """
        Read in all unread lines and return them as a list.
        """
        return [line for line in self]

    def read(self):
        """
        Read in all unread lines and return them as a single string.
        """
        lines = self.readlines()
        if lines:
            try:
                return ''.join(lines)
            except TypeError:
                return ''.join(force_text(line) for line in lines)
        else:
            return None

    def _is_closed(self):
        if not self._fh:
            return True
        try:
            return self._fh.closed
        except AttributeError:
            if isinstance(self._fh, gzip.GzipFile):
                # python 2.6
                return self._fh.fileobj is None
            else:
                raise

    def _parse_offset_file(self):
        # if offset file exists and non-empty, open and parse it
        if exists(self._offset_file) and getsize(self._offset_file):
            offset_fh = open(self._offset_file, "r")
            offset_data = [line.strip() for line in offset_fh]
            offset_fh.close()
            self._offset_file_inode = int(offset_data[0])
            self._offset = int(offset_data[1])
            self._last_log = datetime.strptime(offset_data[2], self._dt_format)
        else:
            self._offset = 0

    def _get_offset(self):
        if self._offset is None:
            self._parse_offset_file()

        return self._offset

    def _filehandle(self):
        """
        Return a filehandle to the file being tailed, with the position set
        to the current offset.
        """
        if not self._fh or self._is_closed():
            if self._rotated_logfiles:
                filename = self._rotated_logfiles.pop(0)
            else:
                filename = self.filename

            if filename.endswith('.gz'):
                self._fh = gzip.open(filename, 'r')
            else:
                self._fh = open(filename, "r")

            self._fh.seek(self._get_offset())

        return self._fh

    def _update_offset_file(self):
        """
        Update the offset file with the current inode and offset.
        """
        if self._is_closed():
            return
        offset = self._filehandle().tell()
        inode = stat(self.filename).st_ino
        fh = open(self._offset_file, "w")
        fh.write(
            "%s\n%s\n%s\n" % (
                inode,
                offset,
                datetime.now().strftime(self._dt_format)
            )
        )
        fh.close()

    def _determine_rotated_logfiles(self):
        """
        Looks up the rotated files and returns them.
        """
        end = datetime.now().replace(minute=0, second=0, microsecond=0)
        start = self._last_log.replace(minute=0, second=0, microsecond=0)
        elapsed_hours = int((end - start).total_seconds()) / 60 / 60

        if not elapsed_hours:
            return []

        files_list = []
        while start < end:
            candidate = self._filename_format % {
                'filename': self.filename,
                'host_name': self._hostname,
                'log_hour': start.strftime(self._log_hour_format),
            }

            if exists(candidate):
                files_list.append(candidate)
            start += timedelta(hours=1)

        return files_list

    def _reload(self):
        self._fh.close()
        self._offset = 0

    def _check_rotate_truncate(self):
        fh = self._filehandle()
        start_pos = fh.tell()
        fh_ino = fstat(fh.fileno()).st_ino

        try:
            fh_stat = stat(self.filename)
        except OSError:
            logging.info("File moved, reloading...")
            self._reload()
            return

        current_ino = fh_stat.st_ino
        current_size = fh_stat.st_size

        if fh_ino != current_ino:
            logging.info("File rotated, reloading...")
            self._reload()

        if self.copytruncate and (current_size < start_pos):
            logging.info("File truncated, reloading...")
            self._reload()

    def _wait_for_update(self):
        while(self.wait_timeout < 0 or self.time_waited < self.wait_timeout):
            time.sleep(self.wait_step)
            self.time_waited += self.wait_step
            line = self._filehandle().readline()
            if line:
                self.time_waited = 0.0
                return line
            self._check_rotate_truncate()
        else:
            raise StopIteration

    def _get_next_line(self):
        line = self._filehandle().readline()
        if not line:
            if self._catching_up:
                raise StopIteration
            self._check_rotate_truncate()
            return self._wait_for_update()
        return line

    def exit_handler(self, signal, frame):
        logging.info("Received exit signal, shutting down...")
        sys.exit(0)


def main():
    logging.basicConfig(level=logging.DEBUG)

    # command-line parsing
    cmdline = OptionParser(
        usage="usage: %prog [options] logfile",
        description="Print log file lines that have not been read."
    )
    cmdline.add_option(
        "--offset-file", "-o", action="store",
        help="File to which offset data is written"
             " (default: <logfile>.offset)."
    )
    cmdline.add_option(
        "--paranoid", "-p", action="store_true",
        help="Update the offset file every time we read a line (as opposed to"
             " only when we reach the end of the file)."
    )
    cmdline.add_option(
        "--no-copytruncate", action="store_true",
        help="Don't support copytruncate-style log rotation. Instead, if the"
             " log file shrinks, print a warning."
    )
    cmdline.add_option(
        "--version", action="store_true",
        help="Print version and exit."
    )

    options, args = cmdline.parse_args()

    if options.version:
        print("pygtail version", __version__)
        sys.exit(0)

    if (len(args) != 1):
        cmdline.error("Please provide a logfile to read.")

    pygtail = Pygtail(args[0],
                      offset_file=options.offset_file,
                      paranoid=options.paranoid,
                      copytruncate=not options.no_copytruncate)

    signal.signal(signal.SIGINT, pygtail.exit_handler)
    signal.signal(signal.SIGTERM, pygtail.exit_handler)

    for line in pygtail:
        sys.stdout.write(line)


if __name__ == "__main__":
    main()
