#!/usr/bin/env python

import unittest
from mmtfPyspark.interactions import InteractionFilter

class InteractionFilterTest(unittest.TestCase):

    def setUp(self):
        pass


    def test1(self):
        inFilter = InteractionFilter()
        inFilter.set_query_groups(True, ["HOH","ZN"])

        self.assertTrue(inFilter.is_query_group('ZN'))
        self.assertTrue(inFilter.is_query_group('HOH'))
        self.assertFalse(inFilter.is_query_group('MN'))


    def test2(self):
        inFilter = InteractionFilter()
        inFilter.set_query_groups(False, ["HOH","ZN"])

        self.assertFalse(inFilter.is_query_group('ZN'))
        self.assertFalse(inFilter.is_query_group('HOH'))
        self.assertTrue(inFilter.is_query_group('MN'))


    def test3(self):
        inFilter = InteractionFilter()
        inFilter.set_target_groups(True, ["HOH","ZN"])

        self.assertTrue(inFilter.is_target_group('ZN'))
        self.assertTrue(inFilter.is_target_group('HOH'))
        self.assertFalse(inFilter.is_target_group('MN'))


    def test4(self):
        inFilter = InteractionFilter()
        inFilter.set_target_groups(False, ["HOH","ZN"])

        self.assertFalse(inFilter.is_target_group('ZN'))
        self.assertFalse(inFilter.is_target_group('HOH'))
        self.assertTrue(inFilter.is_target_group('MN'))


    def test5(self):
        inFilter = InteractionFilter()

        self.assertTrue(inFilter.is_query_group('ZN'))
        self.assertTrue(inFilter.is_target_group('ZN'))
        self.assertTrue(not inFilter.is_prohibited_target_group('ZN'))


    def test6(self):
        inFilter = InteractionFilter()
        inFilter.set_query_elements(True, ["N","O"])

        self.assertTrue(inFilter.is_query_element('O'))
        self.assertTrue(inFilter.is_query_element('N'))
        self.assertFalse(inFilter.is_query_element('S'))


    def test7(self):
        inFilter = InteractionFilter()
        inFilter.set_query_elements(False, ["N","O"])

        self.assertFalse(inFilter.is_query_element('O'))
        self.assertFalse(inFilter.is_query_element('N'))
        self.assertTrue(inFilter.is_query_element('S'))


    def test8(self):
        inFilter = InteractionFilter()
        inFilter.set_target_elements(True, ["N","O"])

        self.assertTrue(inFilter.is_target_element('O'))
        self.assertTrue(inFilter.is_target_element('N'))
        self.assertFalse(inFilter.is_target_element('S'))


    def test9(self):
        inFilter = InteractionFilter()
        inFilter.set_target_elements(False, ["N","O"])

        self.assertFalse(inFilter.is_target_element('O'))
        self.assertFalse(inFilter.is_target_element('N'))
        self.assertTrue(inFilter.is_target_element('S'))


    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
