#!/usr/bin/env python
#

'''Memo (Palm) to Notes'''


import sys
import optparse
import csv
import sqlite3
import time
import random


PALM_NEWLINE = '\r\r\n'
NOTES_NEWLINE = '\n'


def openfile(filename, mode, defname, defval):
    if filename == defname:
        f = defval
    else:
        f = open(filename, mode)
    return f


def closefile(f):
    if f not in [sys.stdin, sys.stdout, sys.stderr]:
        f.close()


def createId():
    # 'id' is a Python reserved word
    iid = hex(random.getrandbits(128))[2:-1].upper()
    if len(iid) < 32:
        iid = '0' * (32 - len(iid)) + iid
    return iid


def getNotes(ifp):
    notes = []
    reader = csv.DictReader(ifp,
        fieldnames = ['Note', 'Locked', 'CategoryName',],
        lineterminator = PALM_NEWLINE,
        quoting = csv.QUOTE_ALL,
        )
    for row in reader:
        locked = int(row['Locked'])
        if locked:
            locked = 1
        else:
            locked = 0
        notes.append({
            'body': row['Note'],
            'locked': locked,
            'categoryName': row['CategoryName'],
        })
    return notes


def getCategories(cursor):
    c = {}
    cursor.execute('''
        SELECT id, name, color, modified, order_ FROM categories
        ''')
    for row in cursor:
        c[row['name']] = {
            'id': row['id'],
            'color': row['color'],
            'modified': row['modified'],
            'order_': row['order_'],
        }
    return c


def updateCategories(cursor, categories, notes, options):
    minColor = 1
    maxColor = 6
    nextColor = max(
        [category['color'] for name, category in categories.items()]
        ) + 1
    if nextColor > maxColor:
        nextColor = minColor
    nextOrder = max(
        [category['order_'] for name, category in categories.items()]
        ) + 1
    newCategories = []

    for note in notes:
        categoryName = note['categoryName']
        if categoryName not in categories:
            newCategories.append(categoryName)
            categories[categoryName] = {
                'id': createId(),
                'color': nextColor,
                'modified': time.time(),
                'order_': nextOrder,
            }
            nextColor = nextColor + 1
            if nextColor > maxColor:
                nextColor = minColor
            nextOrder = nextOrder + 1

    random.seed()
    for name in newCategories:
        category = categories[name]
        q = '''
            INSERT INTO categories(id, name, color, modified, order_)
            VALUES (X'%s', ?, ?, ?, ?)
            ''' % (category['id'],)
        t = (
            name,
            category['color'],
            category['modified'],
            category['order_'],
            )
        cursor.execute(q, t)

    if newCategories:
        q = '''
            UPDATE data
            SET categoryModified = ?
            '''
        t = (categories[newCategories[-1]]['modified'],)
        cursor.execute(q, t)
        if not options.quiet:
            print 'Added %d categories' % len(newCategories)


def reportFailures(notes):
    print 'Failed to import %d memo(s):' % len(notes)
    for note in notes:
        print '%s (%s)' % (note['subject'], note['categoryName'],)


def writeNotes(cursor, categories, notes, options):
    failed = []
    for note in notes:
        note['body'] = note['body'].replace(PALM_NEWLINE, NOTES_NEWLINE)

        note['id'] = createId()
        note['subject'] = note['body'].split(NOTES_NEWLINE, 1)[0].strip()
        note['category'] = categories[note['categoryName']]
        note['modified'] = time.time()
        q = '''
            INSERT INTO memos(id, subject, category, modified, locked, body)
            VALUES (X'%s', ?, X'%s', ?, ?, ?)
            ''' % (note['id'], note['category']['id'],)
        t = (
            note['subject'],
            note['modified'],
            note['locked'],
            note['body'],
            )
        try:
            cursor.execute(q, t)
        except:
            failed.append(note)
            if not options.force:
                reportFailures(failed)
                raise
    if failed:
        reportFailures(failed)
        return -1
    if not options.quiet:
        print 'Imported %d memo(s)' % len(notes)
    return 0


def convert(ifp, cursor, options):
    notes = getNotes(ifp)
    categories = getCategories(cursor)
    updateCategories(cursor, categories, notes, options)
    return writeNotes(cursor, categories, notes, options)


def main():
    p = optparse.OptionParser(
        usage = '%prog [options]',
        description = 'Convert Palm Memo CSV export to Notes SQLite database',
        )
    p.add_option('-i', '--input',
        dest = 'input',
        metavar = 'FILENAME',
        help = 'Input Palm Memo CSV export filename (default: %default)',
        )
    p.add_option('-d', '--dbname',
        dest = 'dbname',
        metavar = 'FILENAME',
        help = 'SQLite3 database filename (default: %default)',
        )
    p.add_option('-f', '--force',
        dest = 'force',
        action = 'store_true',
        help = 'Ignore errors (default: %default)',
        )
    p.add_option('-q', '--quiet',
        dest = 'quiet',
        action = 'store_true',
        help = 'Suppress output (default: %default)',
        )
    p.set_defaults(
        input = '-',
        dbname = 'User.db',
        force = False,
        quiet = False,
        )
    options, args = p.parse_args()

    # Allow a single arguments to be used as input and dbname filenames, if
    # they were not already specified.
    if len(args) > 0 and options.input == '-':
        options.input = args[0]
    if len(args) > 1 and options.dbname == '-':
        options.dbname = args[1]
    if not options.dbname:
        p.print_help()
        return 1

    inputf = openfile(options.input, 'rb', '-', sys.stdin)
    try:
        dbconn = sqlite3.connect(options.dbname)
        try:
            dbconn.row_factory = sqlite3.Row
            dbconn.text_factory = str
            cursor = dbconn.cursor()
            try:
                rc = convert(inputf, cursor, options)
            finally:
                cursor.close()
            if rc == 0:
                dbconn.commit()
        finally:
            dbconn.close()
    finally:
        closefile(inputf)
    return 0


if __name__=='__main__':
    sys.exit(main())
