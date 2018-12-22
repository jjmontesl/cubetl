import doctest
import unittest

from incf.countryutils import transformations


class IsStringTestCase(unittest.TestCase):

    def test_string(self):
        self.assertTrue(transformations.is_string_type(''))

    def test_not_a_string(self):
        self.assertFalse(transformations.is_string_type(42))


class CcaToCcnTestCase(unittest.TestCase):

    def test_should_raise_due_to_invalid_key(self):
        with self.assertRaises(KeyError):
            transformations.cca_to_ccn('error')


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite('README.txt',
                                       optionflags=doctest.ELLIPSIS))
    return suite
