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
		self.config = agol.load_config()
		self.token = agol.generate_token(self.config.get('user'), self.config.get('pass'))

	def test_load_config(self):
		config = agol.load_config()
		assert isinstance(config, dict)

	def test_should_load_config_from_user_home(self):
		config = agol.load_config('jar jar binks')
		assert isinstance(config, dict)

	def test_dataframe_to_featureset(self):
		result = agol.dataframe_to_featureset(self.df)
		assert result

	def test_able_to_generate_token(self):
		token = agol.generate_token(self.config.get('user'), self.config.get('pass'))
		assert token is not None

	def test_get_credits_count(self):
		count = agol.get_credits_count(self.config.get('org_url'), self.token)
		assert isinstance(count, float) and count >= 0.0

	def test_query(self):
		pass
