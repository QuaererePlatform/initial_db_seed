import logging
import os
import pathlib
import yaml

import arango

LOGGER = logging.getLogger()
WEB_SITE_DB_COLLECTION = 'WebSites'
WEB_SITE_URLS_FILE_NAME = 'web_site_urls.yaml'


def setup_logging():
    pass


def main():
    web_site_urls_file = pathlib.Path(WEB_SITE_URLS_FILE_NAME)
    with web_site_urls_file.open() as fh:
        web_site_info = yaml.load(fh)

    a_client = arango.ArangoClient()
    q_db = a_client.db('quaerere',
                       os.getenv('ARANGODB_USER'),
                       os.getenv('ARANGODB_PASSWD'))

    if not q_db.has_collection(WEB_SITE_DB_COLLECTION):
        LOGGER.info(f'Creating website collection.',
                    extra={'website_collection': WEB_SITE_DB_COLLECTION})
        q_db.create_collection(WEB_SITE_DB_COLLECTION)

    web_sites = q_db.collection(WEB_SITE_DB_COLLECTION)
    for web_site in web_site_info['web_sites']:
        db_cur = web_sites.find({'url': web_site['url']})
        if db_cur.count() == 0:
            LOGGER.info('Inserting web site.', extra={'web_site': web_site})
            web_sites.insert(web_site)
        else:
            LOGGER.info('Found web site in DB, skipping',
                        extra={'web_site': web_site})


if __name__ == '__main__':
    main()
