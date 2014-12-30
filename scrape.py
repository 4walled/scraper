#!/usr/bin/python

import hashlib
import os
import re
import subprocess
import sys
import threading
import urllib2

import Image
import MySQLdb
import PySQLPool
import multiprocessing
import lxml.html
from BeautifulSoup import UnicodeDammit

######################[ Config ]############################
# List of scrapable boards.  Should be in the format {board : url}
BOARDS = {
        "w": "http://boards.4chan.org/w/",
        "wg": "http://boards.4chan.org/wg/",
        "hr": "http://boards.4chan.org/hr/",
        "7chan": "http://7chan.org/wp/",
        "v": "http://boards.4chan.org/v/",
        "htmlnew": "http://boards.4chan.org/htmlnew/",
        }

# Total number of pages in board to scrape
MAX_PAGES = 2

# Total number of download threads to be run at a time.
MAX_DOWNLOADS = 4

# Max number of MySQL connections
# 10 is the default
MAX_MYSQL = 10

# Location to save downloaded images
D_LOC = "//home/site/domains/4walled.cc/images/"

# Thumbnail directory
TH_LOC = "/home/site/domains/4walled.cc/thumbnails/"

# Thumbnail max dimensions:
TH_SIZE = (250, 250)

# Information used to connect to the database
DB_CONNECTION = {
        "host": "HOSTNAME",
        "user": "USERNAME",
        "passwd": "PASSWORD",
        "db": "DATABASE"
        }

######################[ /Config ]###########################


######################[ Globals ]###########################
# Don't touch these kthxbai

# MySQL Connection
global CONNECTION
CONNECTION = None

# DB ids for the Source table
global SOURCE_IDS
SOURCE_IDS = {
    "w": 1,
    "wg": 2,
    #"7chan": 3,
    "hr": 4,
    #"htmlnew": 5,
}

######################[ /Globals ]##########################

def decode_html(html_string):
    converted = UnicodeDammit(html_string, isHTML=True)
    if not converted.unicode:
        raise UnicodeDecodeError(
            "Failed to detect encoding, tried [%s]",
            ', '.join(converted.triedEncodings))
    # print converted.originalEncoding
    return converted.unicode

def striptags(args):
    """ Removes tags and whitespace from all args """
    for key, value in args.iteritems():
        # If no value has been found, use empty string.
        # Mostly used for tripcode
        if not value:
            value = ""
        else:
            value = re.sub(r"<.*?>", "", value)
            value = re.sub(r"\s*", "", value)
        args[key] = value
    return args

def gimmeContents(url):
    """ Fetches HTML, pretends to be Firefox.  Returns string of HTML """
    request = urllib2.Request(url,
            headers={"User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 (.NET CLR 3.5.30729)"})
    try:
        connection = urllib2.urlopen(request)
    except urllib2.URLError:
        print("Could not connect to %s" % (url,))
        return
    else:
        contents = connection.read()
    return contents

def getThreads(boardname, boardurl, pageurl):
    """
    Scrapes board for threads.
    For each thread, call Download to get images
    """
    pagecontents = gimmeContents(pageurl)
    pagecontents = decode_html(pagecontents)
    doc = lxml.html.fromstring(pagecontents)
    doc.make_links_absolute(boardurl)
    for thread in doc.find_class("thread"):
        for link in thread.find_class("replylink"):
            yield link.get("href")

def markScraped(sql, chan_id, board):
        sql.Query("""INSERT INTO scraped (image_id, source_id) VALUES (%s, %s)""", (chan_id, SOURCE_IDS[board]))


def download(board, username, tripcode, resolution, tag, url, chan_id):
    """
    Checks to see if file is already downloaded.
    If not, downloads file, md5's it, thumbnails,
    and adds to database
    """

    try:
        global CONNECTION
        PySQLPool.getNewPool().maxActiveConnections = MAX_MYSQL
        CONNECTION = PySQLPool.getNewConnection(**DB_CONNECTION)
    
        ext = os.path.splitext(url)[1].lower()
        sql = PySQLPool.getNewQuery(CONNECTION)
        image = gimmeContents(url)
        md5 = hashlib.md5(image).hexdigest()
        print("md5=%s Processing image" % (md5))
    
        # Check for embedded script hack code
        if image.find("ActiveXObject") != -1:
            print("Found script code image")
            return

        """ Compares hash value to check for duplicate image """
        sql.Query("""SELECT id FROM Image WHERE md5=%s""", (md5,))
        if bool(sql.affectedRows):
            print("md5=%s Image is a duplicate with id=%s, skipping" % (md5, chan_id))
            markScraped(sql, chan_id, board)
            return

        imagefilename = str(md5) + ext
        imagefilepath = os.path.join(D_LOC, imagefilename[:2], imagefilename)
        open(imagefilepath, "wb").write(image)
    
        """ Creates a thumbnail image """
        thumbfilename = str(md5) + ".jpg"
        thumbfilepath = os.path.join(TH_LOC, thumbfilename[:2], thumbfilename)
        
        if os.path.isfile(imagefilepath):
            try:
                im = Image.open(imagefilepath)
                im.thumbnail(TH_SIZE, Image.ANTIALIAS)
                im.save(thumbfilepath, "JPEG")
            except IOError:
                print("Cannot create thumbnail (%s), deleted" % imagefilepath)
        
        """
        Insert information into the database.
        """
        try:
            # Check for existing poster/trip combination
            sql.Query("""SELECT id FROM Poster WHERE name=%s AND tripcode=%s""", (username, tripcode))
            if sql.affectedRows:
                # record should be a tuple of dicts, we want the id of the first one
                poster_id = sql.record[0]['id']
            else:
                # 0 affected rows: no preexisting user/trip.  Make one
                sql.Query("""INSERT INTO Poster (name, tripcode) VALUES (%s, %s)""", (username, tripcode))
                poster_id = sql.lastInsertID
                
            # Check for existing tag
            tag_id = None
            if tag != "":
                tag = tag.encode("utf-8")
                sql.Query("""SELECT id FROM Tag WHERE name=%s""", (tag,))
                if sql.affectedRows:
                    tag_id = sql.record[0]['id']
                else:
                    # Tag doesn't exist :( Create new tag row
                    sql.Query("""INSERT INTO Tag (name) VALUES (%s)""", (tag,))
                    tag_id = sql.lastInsertID
                
            sql.Query("""INSERT INTO Image (poster_id, source_id, md5, extension, width, height, aspect_ratio, downloads, date_added, rating)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, 0, NOW(), NULL)""", (poster_id, SOURCE_IDS[board], md5, 
                                                                                    ext[1:], resolution[0],
                                                                                    resolution[1], int((float(resolution[0])/float(resolution[1])) * 100)))
            image_id = sql.lastInsertID
            if tag_id:
                sql.Query("""INSERT INTO Tag_Image (image_id, tag_id) VALUES (%s, %s)""", (image_id, tag_id))
            markScraped(sql, chan_id, board)
        except MySQLdb.Error as e:
            ## Delete image and thumb, undo any potential inserts
            #try:
            #    os.remove(imagefilepath)
            #    os.remove(thumbfilepath)
            #except (OSError, MySQLdbError):
            #    # Thumb or image doesn't exist, or MySQL broke a second time :(
            #    print("Failed to insert, then failed to remove the image")
            #    return
            print("Could not insert image into database " + str(e), sys.exc_info(), sys.exc_info()[2].tb_lineno)
            return
    except:
        print("Unexpected error:", sys.exc_info(), sys.exc_info()[2].tb_lineno)
        return
    print("md5=%s Image inserted successfully with id=%s" % (md5, image_id))

def checkConfig():
    """ Ensures config variables are sane. """
    message = ""
    dne = "' does not exist or permission denied"
    if MAX_PAGES > 10:
        message = "MAX_PAGES cannot be more than 10"
    elif not os.path.exists(D_LOC) or not os.path.isdir(D_LOC):
        message = "D_LOC location '" + D_LOC + dne
    elif not os.path.exists(TH_LOC) or not os.path.isdir(TH_LOC):
        message = "TH_LOC location '" + TH_LOC + dne
    elif MAX_MYSQL < 1:
        message = "MAX_MYSQL cannot be less than 1"
    if message:
        print(message)
        return
    try:
        MySQLdb.connect(**DB_CONNECTION)
    except MySQLdb.MySQLError:
        print("Database connection error.  Try adjusting DB_CONNECTION")

if __name__ == "__main__":
    # Set image permissions to 0755
    os.umask(18) # octal 022
    checkConfig()
    
    pool = multiprocessing.Pool(MAX_DOWNLOADS)
    
    # Check commandline args for board
    #if len(sys.argv) > 1:
    #    board = sys.argv[1]
    #    if not board in BOARDS:
    #        print("Invalid board.  Available boards are: " + 
    #                ", ".join(BOARDS.keys()))
    #        sys.exit(1)
    #else:
    #    board = "w"

    # Initiate the MySQL Pool
    global CONNECTION
    PySQLPool.getNewPool().maxActiveConnections = MAX_MYSQL
    CONNECTION = PySQLPool.getNewConnection(**DB_CONNECTION)

    for board in SOURCE_IDS:
        for page in range(MAX_PAGES):
            for threadurl in getThreads(board, BOARDS[board], "%s%s.html" % (BOARDS[board], page)):
                print("Scraping the board thread %s" % threadurl)
                threadpagecontent = gimmeContents(threadurl)
                doc = lxml.html.fromstring(threadpagecontent)
                doc.make_links_absolute(threadurl)
                for post in doc.find_class("postContainer"):
                    username, tripcode, resolution, tag, url = ["" for x in range(5)]
                    
                    try:
                        username = post.find_class("name")[0].text
                        if username is None or username == "":
                            username = "Anonymous"
                    except:
                        pass
                    
                    if username is None or username == "":
                        print("username is not found thread is %s" % (threadurl,))
                        print(lxml.etree.tostring(post))
                        sys.exit(1)
                    
                    try:
                        tripcode = post.find_class("postertrip")[0].text
                    except:
                        pass
    
                    try:
                        resolution = re.search("(\d+)x(\d+)", post.find_class("fileText")[0].text_content()).groups()
                        if (resolution[0] * resolution[1]) < 300000: # 93.75% of 800*400
                            continue # skip to the next post
                        if not .25 < (float(resolution[0]) / float(resolution[1])) < 4:
                            continue # image is very tall or very wide, skip
                    except:
                        pass
    
                    try:
                        tag = post.find_class("fileText")[0].find("span").text_content()
                        if re.match("\d+\.(png|jpg|gif)", tag, re.IGNORECASE):
                            tag = ""
                    except:
                        pass
    
                    try:
                        url = post.find_class("fileThumb")[0].get("href")
                        if url is None:
                            if post.find_class("fileThumb")[0][0].get("href").find("deleted") != -1:
                                continue
                            print("Something unexpected happened!")
                            sys.exit(1)
                    except:
                        #print("Failed to find the URL for the image associated with a post, bailing")
                        continue # If we can't get a URL this post is text only.
    
                    # Make sure we have not already downloaded an image from this URL
                    try:
                        chan_id = url.split("/")[-1].split(".")[0]
                        sql = PySQLPool.getNewQuery(CONNECTION)
                        sql.Query("""SELECT image_id FROM scraped WHERE image_id=%s AND source_id=%s""", (chan_id, SOURCE_IDS[board]))
                        if sql.affectedRows == 0:
                            # "%s|%s|%s|%s|%s" % (username, tripcode, resolution, tag, url)
                            pool.apply_async(download, (board, username, tripcode, resolution, tag, url, chan_id))
                    except:
                        print("Unexpected error:", sys.exc_info(), sys.exc_info()[2].tb_lineno)
                        print(lxml.etree.tostring(post, pretty_print=True))
                        sys.exit(1)
    pool.close()
    print("Waiting for images to be processed...")
    pool.join()
    print("Scraping done")


