from calendar import timegm
from datetime import datetime, timedelta
from hashlib import md5
import os, os.path
import urllib2

import feedparser
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Article(Base):
    "The sqlalchemy DB model for articles"
    __tablename__ = 'article'
    id = Column(String, primary_key = True)
    timestamp = Column(Integer, index = True)

    def __init__(self, id, timestamp):
        self.id = id
        self.timestamp = timestamp

class FeedArticleProxy(object):
    def __init__(self, article):
        self.article = article

    def __getattr__(self, attr):
        return getattr(self.article, attr)

    def digest(self, itm):
        "Some GUID equivelants need hashing"
        return md5(itm).hexdigest()

    def tracker_guid(self):
        if 'guid' in self.article:
            return self.article.guid
        elif 'id' in self.article:
            return self.article.id
        elif self.get('guidislink', False):
            return self.digest(self.article.link)
        elif 'summary' in self.article:
            return self.digest(self.article.summary)
        else:
            return self.digest(self.article.link)

    def save(self):
        timestamp = timegm((datetime.now()).timetuple())
        self.sqlalchemy_session.add(Article(self.tracker_guid(), timestamp))
        self.sqlalchemy_session.commit()

    def is_read(self):
        existing = self.sqlalchemy_session.query(Article).filter_by(id = self.tracker_guid()).first()
        return bool(existing)

class RSSTracker(object):
    def __init__(self,
                 url,
                 fname,
                 debug=False,
                 keepfor=timedelta(days=7),
                 tmpfname = None):
       
        engine = create_engine('sqlite:///%s' % fname, echo = debug)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)

        self.url = url
        self.session = Session() 
        self.debug = debug
        self.keepfor = keepfor
        self.tmpfname = tmpfname

        # clean up first, it will make querying cheaper
        self.cleanup()

    def cleanup(self):
        expiry = timegm((datetime.now() - self.keepfor).timetuple())
        self.session.query(Article).filter(Article.timestamp < expiry).delete()

    def entries(self, only_unread = True):
        #if self.tmpfname and os.path.isfile(self.tmpfname):
        #    mtime = os.path.getmtime(self.tmpfname)
        #    print mtime

        e = feedparser.parse(self.url)
        for entry in e.entries:
            # upgrade to our special class that can save itself
            entry = FeedArticleProxy(entry)
            entry.sqlalchemy_session = self.session

            if ((only_unread and not entry.is_read())
                or not only_unread):
                yield entry

    __iter__ = entries

if __name__ == '__main__':
    for entry in RSSTracker('http://rss.reddit.com', './test.db', tmpfname = '/tmp/rsstracker'):
        print repr(entry.title), repr(entry.link)
        entry.save()
