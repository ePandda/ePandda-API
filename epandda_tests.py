import os
import unittest
import json
import time
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from selenium import webdriver, common
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

import api

class epanddaTests(unittest.TestCase):

    def setUp(self):
        api.app.testing = True
        self.app = api.app.test_client()
        self.config = json.load(open('./config.json'))

        self.chromeOptions = webdriver.ChromeOptions()
        self.chromeOptions.add_argument("--headless")
        self.chromeOptions.add_argument("window-size=1920,1080")
        self.chromeOptions.add_argument("--no-sandbox")
        self.driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=self.chromeOptions)

    def tearDown(self):
        api.app.testing = False
        self.app = None
        time.sleep(5)
        self.driver.close()
        self.driver.quit()

    #
    # DB Section
    # Test the status of the necessary databases (ElasticSearch and MongoDB)
    #
    def test_elastic(self):
        es = Elasticsearch([self.config['elastic_host']], timeout=30)
        try:
            res = es.search()
        except ConnectionError:
            self.fail("Could not connect to elasticsearch!")
        self.assertTrue(es.indices.exists('idigbio'), 'idigbio index missing in elasticsearch')
        self.assertTrue(es.indices.exists('pbdb'), 'pbdb index missing in elasticsearch')

    def test_mongo(self):
        client = MongoClient("mongodb://localhost:27017")
        db_names = client.database_names()
        # TODO
        # 1) Assert that necessary dbs exist
        # 2) Assert that necessary collections exist within each db
        # 3) Make sure there are valid docs in there

    #
    # API Section
    # The tests below verify that the ePandda API is up and returning
    #
    def test_banner(self):
        res = self.app.get('/')
        banner_json = json.loads(res.get_data(as_text=True))
        self.assertTrue('routes' in banner_json and len(banner_json['routes']) > 0, 'ePandda endpoints missing')
        for route in banner_json['routes']:
            self.assertTrue('url' in route and 'name' in route and 'description' in route and 'methods' in route, route['name'] + 'missing fields')

    def test_stats(self):
        res = self.app.get('/stats?totalRecords=true&lastUpdated=true')
        stats_json = json.loads(res.get_data(as_text=True))
        self.assertTrue('success' in stats_json, 'Did not successfully retrieve stats')
        results = stats_json['results']
        self.assertTrue(results and 'lastUpdated' in results and 'totalRecords' in results, 'Missing stats')

    def test_occurrences(self):
        res = self.app.get('/occurrences?terms=genus:hadrosaurus&returnMedia=True')
        occ_json = json.loads(res.get_data(as_text=True))

        queryInfo = occ_json['queryInfo']
        self.assertGreaterEqual(queryInfo['idigbioTotal'], 136, "Expected at least 136 iDigBio records to be returned")
        self.assertGreaterEqual(queryInfo['pbdbTotal'], 12, "Expected at least 12 PBDB records to be returned")

        results = occ_json['results']
        self.assertTrue('92ddf5bb7cbc4b0b12f09dd2d894758f23ad7d31' in results)
        if '92ddf5bb7cbc4b0b12f09dd2d894758f23ad7d31' in results:
            matchTest = results['92ddf5bb7cbc4b0b12f09dd2d894758f23ad7d31']
            self.assertTrue(len(matchTest['matches']) >= 7 and matchTest['totalMatches'] >= 7, "Missing matching records")

    #
    # Website tests
    # Test to see if the site is up and working as intended (uses selenium)
    # At present runs basic tests on each of the 4 main epandda.org pages
    # Could/should be extended to test sending emails/more in depth form testing
    #
    def test_homepage(self):
        try:
            self.driver.get("https://epandda.org")
        except Exception as e:
            self.fail("Could not load epandda.org")

        recordCountEl = self.driver.find_element_by_id("status_numRecords")
        recordCount = int(recordCountEl.text.replace(',', ''))
        self.assertGreater(recordCount, 0, "Failed to count up records")

        idbCountEl = self.driver.find_element_by_id("nowSearching_idb")
        idbCount = int(idbCountEl.text.replace(',', ''))
        self.assertGreater(idbCount, 0, "Failed to count IDB records")

        pbdbCountEl = self.driver.find_element_by_id("nowSearching_pbdb")
        pbdbCount = int(pbdbCountEl.text.replace(',', ''))
        self.assertGreater(idbCount, 0, "Failed to count PBDB records")

    def test_documentation(self):
        try:
            self.driver.get("https://epandda.org/#documentation")
        except Exception as e:
            self.fail("Could not load Documentation")

        overviewEl = self.driver.find_element_by_id("overview")
        self.assertEqual(overviewEl.text, "Overview", "Failed to load Overview documentation")

    def test_sandbox(self):
        try:
            self.driver.get("https://epandda.org/#sandbox")
        except Exception as e:
            self.fail("Could not load Sandbox")

        time.sleep(5)
        queryForm = self.driver.find_element_by_id("apiForm")
        termField = queryForm.find_element_by_name("terms")
        if not termField:
            self.fail("Could not load Occurrence search form")
        else:
            termField.clear()
            termField.send_keys("genus:hadrosaurus")
            termField.send_keys(Keys.RETURN)
            time.sleep(20)

            resultsContainer = self.driver.find_element_by_id("apiResultsContainer")
            resultsCounts = resultsContainer.find_element_by_id("apiResultsCounts")
            if len(resultsCounts.text) < 1:
                self.fail("Failed to load search Results")

    def text_examples(self):
        try:
            self.driver.get("https://epandda.org/#examples")
        except Exception as e:
            self.fail("Could not load examples")

        mapContainer = self.driver.find_element_by_id("map")
        if not mapContainer:
            self.fail("Failed to Load 'Fossils In My Backyard Example'")

        imageForm = self.driver.find_element_by_id("taxonTerm")
        imageForm.clear()
        imageForm.send_keys("hadrosarus")
        imageSelect = Select(self.driver.find_element_by_id("taxonTerm"))
        imageSelect.select_by_visible_text("genus")
        imageForm.send_keys(Keys.RETURN)
        time.sleep(30)
        imageReturns = self.driver.find_element_by_id("idbImages")
        imageDivs = imageReturns.find_elements_by_class_name("imageResult")
        if len(imageDivs) < 48:
            self.fail("Failed to search iDigBio images example")


if __name__ == '__main__':
    unittest.main()
