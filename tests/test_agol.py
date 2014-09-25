import pandas

from nose.tools import assert_raises, assert_almost_equal
from agolpandas import agol

class TestAgol():

	def setUp(self):
		data = {}
		data['str_test'] = 'test string'
		data['int_test'] = 1
		data['float_test'] = 1.0
		data['bool_test'] = True
		data['null_test'] = None
		self.df = pandas.DataFrame([data])

	def test_dataframe_to_featureset(self):
		result = agol.dataframe_to_featureset(self.df)
		assert result 