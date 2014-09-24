# -*- coding: utf-8 -*-

import json
import os
import urllib
import urllib2
import unittest
import numpy
import requests
import pandas
import agol_settings

def chunker(seq, size):
	return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

def featureset_to_dataframe(featureset, convertGeometry=False, useAliases=False):
	items = [x['attributes'] for x in featureset['features']]
	df = pandas.DataFrame(items)
	if useAliases and featureset.get('fieldAliases'):
		df.rename(columns=featureset['fieldAliases'], inplace=True)
	if convertGeometry:
		pass
	return df

def dataframe_to_featureset(dataFrame, xField=None, yField=None, wkid=4326, output='json'):
	dicts = dataFrame.to_dict(outtype='records')
	features = [{'attributes':d} for d in dicts]
	for f in features:
		for attr in f['attributes'].keys():
			v = f['attributes'][attr]
			if v is numpy.nan:
				f['attributes'][attr] = None

	if xField and yField:
		for f in features:
			geom = {}
			geom['x'] = f['attributes'][xField]
			geom['y'] = f['attributes'][yField]
			geom['spatialReference'] = {'wkid' : wkid}
			f['geometry'] = geom

	if output == 'records':
		return features

	elif output == 'json':
		return json.dumps(features)

	else:
		raise Exception('Invalid output type')

def geocode_dataframe(df, geocodeService, token=None, sourceCountry='USA'):
    results = pandas.DataFrame()
    chunkSize = min(20,len(df))
    part_count = max(len(df) / chunkSize, 1)
    completed = 0
    records = df.to_dict('records')
    for c in numpy.array_split(records, part_count, axis=1):
        addresses = {}
        addresses['records'] = [dict([('attributes',r)]) for r in c]

        params = {}
        params['addresses'] = json.dumps(addresses)
        params['sourceCountry'] = sourceCountry
        params['f'] = 'json'
        params['token'] = token

        response = requests.post(geocodeService, data=params)
        response.raise_for_status()
        resp_json = response.json()
        print resp_json
        if not resp_json.get('locations'):
            print 'no locations property found'
            continue

        items = [x['attributes'] for x in resp_json['locations']]
        result_df = pandas.DataFrame(items)
        completed += len(result_df)
        print completed
        if results.empty:
            results = result_df
        else:
            results = results.append(result_df)
            print 'geocode results count {}'.format(len(results))

    return results

def do_post(url, param_dict, proxy_url=None, proxy_port=None):
	""" performs the POST operation and returns dictionary result """
	if proxy_url is not None:
		if proxy_port is None:
			proxy_port = 80
		proxies = {"http":"http://%s:%s" % (proxy_url, proxy_port),
				   "https":"https://%s:%s" % (proxy_url, proxy_port)}
		proxy_support = urllib2.ProxyHandler(proxies)
		opener = urllib2.build_opener(proxy_support, urllib2.HTTPHandler(debuglevel=1))
		urllib2.install_opener(opener)

	request = urllib2.Request(url, urllib.urlencode(param_dict))
	result = urllib2.urlopen(request).read()
	if result == "":
		return ""
	jres = json.loads(result)
	if 'error' in jres:
		if jres['error']['message'] == 'Request not made over ssl':
			if url.startswith('http://'):
				url = url.replace('http://', 'https://')
				return do_post( url, param_dict, proxy_url, proxy_port)

	return jres #todo add unicode conversion?

def generate_token(username, password, referer=None, tokenURL=None, proxy_url=None, proxy_port=None):
	""" generates a token for a feature service """
	referer = r'https://www.arcgis.com'
	tokenUrl  = r'https://www.arcgis.com/sharing/rest/generateToken'

	query_dict = {'username': username,
				  'password': password,
				  'expiration': str(60),
				  'referer': referer,
				  'f': 'json'}

	token = do_post(url=tokenUrl, param_dict=query_dict, proxy_url=proxy_url, proxy_port=proxy_port)

	if "token" not in token:
		return None
	else:
		return token['token']

def add_tags(tags, token):
	url = os.path.join(agol_settings.agol_base, 'sharing/rest/community/users', agol_settings.username, 'tags')
	response = requests.get(url)

def is_service_name_available(name, serviceType, token):
	url = os.path.join(agol_settings.agol_base, 'sharing/rest/portals', agol_settings.agol_id, 'isServiceNameAvailable')
	params = {}
	params['name'] = name
	params['type'] = serviceType
	params['f'] = 'json'
	params['token'] = token

	result = requests.get(url, params=params)
	return result.json()['available']

def create_feature_service(name, token):
	if not is_service_name_available(name, 'Feature Service', token):
		print 'sorry name not available'
		return

	url = os.path.join(agol_settings.agol_base, 'sharing/rest/content/users', agol_settings.agol_user, 'createService')

	editor_tracking_info = {}
	editor_tracking_info['enableEditorTracking'] = False
	editor_tracking_info['enableOwnershipAccessControl'] = False
	editor_tracking_info['allowOthersToUpdate'] = True
	editor_tracking_info['allowOthersToDelete'] = True

	xssPreventionInfo = {}
	xssPreventionInfo['xssPreventionEnabled'] = True
	xssPreventionInfo['xssPreventionRule'] = 'InputOnly'
	xssPreventionInfo['xssInputRule'] = 'rejectInvalid'
	xssPreventionInfo['allowOthersToDelete'] = True

	create_params = {}
	create_params['currentVersion'] = 10.21 #TODO: get current version dynamically
	create_params['serviceDescription'] = '10.21'
	create_params['hasVersionedData'] = False
	create_params['supportsDisconnectedEditing'] = False
	create_params['hasStaticData'] = False
	create_params['maxRecordCount'] = 2000
	create_params['supportedQueryFormats'] = "JSON"
	create_params['capabilities'] = "Query,Editing,Create,Update,Delete"
	create_params['description'] = ''
	create_params['copyrightText'] = ''
	create_params['allowGeometryUpdates'] = True
	create_params['syncEnabled'] = False
	create_params['size'] = 9076736
	create_params['editorTrackingInfo'] = editor_tracking_info
	create_params['xssPreventionInfo'] = xssPreventionInfo
	create_params['tables'] = []
	create_params['_ssl'] = False
	create_params['name'] = name

	params = {}
	params['createParameters'] = {}
	params['targetType'] = 'featureService'
	params['f'] = 'json'
	params['token'] = token

	result = requests.post(url, params=params)
	return result

def create_layer_object():

	editing_info = {}
	editing_info['lastEditDate'] = None

	advanced_query_capabilities = {}
	advanced_query_capabilities['supportsPagination'] = True
	advanced_query_capabilities['supportsQueryWithDistance'] = True
	advanced_query_capabilities['supportsReturningQueryExtent'] = True
	advanced_query_capabilities['supportsStatistics'] = True
	advanced_query_capabilities['supportsOrderBy'] = True
	advanced_query_capabilities['supportsDistinct'] = True
	advanced_query_capabilities['supportsPagination'] = True

	sr = {}
	sr['wkid'] = 102100

	extent = {}
	extent['xmin']
	extent['ymin']
	extent['xmax']
	extent['ymax']
	extent['spatialReference'] = sr

	layer = {}
	layer['currentVersion'] = 10.21
	layer['id'] = 0
	layer['name'] = 'BRA_adm0'
	layer['type'] = 'Feature Layer'
	layer['displayField'] = ''
	layer['description'] = ''
	layer['copyrightText'] = ''
	layer['defaultVisibility'] = True
	layer['type'] = 'Feature Layer'
	layer['editingInfo'] = editing_info
	layer['relationships'] = []
	layer['isDataVersioned'] = False
	layer['supportsCalculate'] = True
	layer['supportsAttachmentsByUploadId'] = True
	layer['supportsRollbackOnFailureParameter'] = True
	layer['supportsStatistics'] = True
	layer['supportsAdvancedQueries'] = True
	layer['advancedQueryCapabilities'] = advanced_query_capabilities
	layer['geometryType'] = 'esriGeometryPolygon'
	layer['minScale'] = 0
	layer['maxScale'] = 0

	layer['extent'] = extent
	layer['supportsStatistics'] = True
	layer['supportsStatistics'] = True
	layer['supportsStatistics'] = True
	layer['supportsStatistics'] = True
	layer['supportsStatistics'] = True
	layer['supportsStatistics'] = True

	return layer

# def update_definition(name, token):
# 	if is_service_name_available(name, 'Feature Service', token):
# 		print 'sorry name is available it does not exist'
# 		return

# 	layer = {}

# 	layers = []



# 	add_to_definition = {}
# 	add_to_definition['layers']

# 	{"layers":["drawingInfo":{"renderer":{"type":"simple","symbol":{"type":"esriSFS","style":"esriSFSSolid","color":[76,129,205,191],"outline":{"type":"esriSLS","style":"esriSLSSolid","color":[0,0,0,255],"width":0.75}},"label":"","description":""},"transparency":0,"labelingInfo":null},"allowGeometryUpdates":true,"hasAttachments":false,"htmlPopupType":"esriServerHTMLPopupTypeNone","hasM":false,"hasZ":false,"objectIdField":"FID","globalIdField":"","typeIdField":"","fields":[{"name":"FID","type":"esriFieldTypeInteger","actualType":"int","alias":"FID","sqlType":"sqlTypeInteger","nullable":false,"editable":false,"domain":null,"defaultValue":null},{"name":"ID_0","type":"esriFieldTypeInteger","actualType":"int","alias":"ID_0","sqlType":"sqlTypeInteger","nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"ISO","sqlType":"sqlTypeNVarchar","length":3,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ENGLI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ENGLI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ISO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ISO","sqlType":"sqlTypeNVarchar","length":54,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_FAO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_FAO","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_LOCAL","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_LOCAL","sqlType":"sqlTypeNVarchar","length":54,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_OBSOL","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_OBSOL","sqlType":"sqlTypeNVarchar","length":150,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_VARIA","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_VARIA","sqlType":"sqlTypeNVarchar","length":160,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_NONLA","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_NONLA","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_FRENC","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_FRENC","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_SPANI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_SPANI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_RUSSI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_RUSSI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ARABI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ARABI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_CHINE","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_CHINE","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"WASPARTOF","type":"esriFieldTypeString","actualType":"nvarchar","alias":"WASPARTOF","sqlType":"sqlTypeNVarchar","length":100,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"CONTAINS_","type":"esriFieldTypeString","actualType":"nvarchar","alias":"CONTAINS","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"SOVEREIGN","type":"esriFieldTypeString","actualType":"nvarchar","alias":"SOVEREIGN","sqlType":"sqlTypeNVarchar","length":40,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISO2","type":"esriFieldTypeString","actualType":"nvarchar","alias":"ISO2","sqlType":"sqlTypeNVarchar","length":4,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"WWW","type":"esriFieldTypeString","actualType":"nvarchar","alias":"WWW","sqlType":"sqlTypeNVarchar","length":2,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"FIPS","type":"esriFieldTypeString","actualType":"nvarchar","alias":"FIPS","sqlType":"sqlTypeNVarchar","length":6,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISON","type":"esriFieldTypeDouble","actualType":"float","alias":"ISON","sqlType":"sqlTypeFloat","nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"VALIDFR","type":"esriFieldTypeString","actualType":"nvarchar","alias":"VALIDFR","sqlType":"sqlTypeNVarchar","length":12,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"VALIDTO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"VALIDTO","sqlType":"sqlTypeNVarchar","length":10,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"EUmember","type":"esriFieldTypeDouble","actualType":"float","alias":"EUmember","sqlType":"sqlTypeFloat","nullable":true,"editable":true,"domain":null,"defaultValue":null}],"types":[],"templates":[{"name":"New Feature","description":"","drawingTool":"esriFeatureEditToolPolygon","prototype":{"attributes":{"ID_0":null,"ISO":null,"NAME_ENGLI":null,"NAME_ISO":null,"NAME_FAO":null,"NAME_LOCAL":null,"NAME_OBSOL":null,"NAME_VARIA":null,"NAME_NONLA":null,"NAME_FRENC":null,"NAME_SPANI":null,"NAME_RUSSI":null,"NAME_ARABI":null,"NAME_CHINE":null,"WASPARTOF":null,"CONTAINS_":null,"SOVEREIGN":null,"ISO2":null,"WWW":null,"FIPS":null,"ISON":null,"VALIDFR":null,"VALIDTO":null,"EUmember":null}}}],"supportedQueryFormats":"JSON","hasStaticData":false,"maxRecordCount":2000,"capabilities":"Query,Editing,Create,Update,Delete","adminLayerInfo":{"geometryField":{"name":"Shape","srid":102100}}}]}


# 	params = {}
# 	params['addToDefinition'] = {}
# 	params['f'] = 'json'
# 	params['token'] = token

# 	result = requests.post(url, params=params)
# 	return result





def create_feature_class(name, serviceType, token):
	available = is_service_name_available(name, serviceType, token)
	if not available:
		print 'sorry service name {} is not available'.format(name)
		return


	# =================================================================
	#data
	# =================================================================
	#http://blueraster.maps.arcgis.com/sharing/rest/content/items/5264708e0b2b4d369916344204665d16/data?f=json&token=5x_BZvtUemIkI1RStEK0H6jEjDKRtUTbmsJjKlCbmUIFaQwEiR4JYS54YVZ1ySEPV2S4w7Uli8WbvMQgTGiGn2cnnE-RARMVlZDB2NWLKdPKpbhLEjmUpVgAbkYeBN-TSHf-CCUqKuqastIQPVHviIIIIAcGY786whMd88g4TVElTos9XFSte-3Nn55FPr_0

	# =================================================================
	#create service POST : 
	# =================================================================
	#http://blueraster.maps.arcgis.com/sharing/rest/content/users/bwc132/createService

	# createParameters:{"currentVersion":10.21,"serviceDescription":"","hasVersionedData":false,"supportsDisconnectedEditing":false,"hasStaticData":false,"maxRecordCount":2000,"supportedQueryFormats":"JSON","capabilities":"Query,Editing,Create,Update,Delete","description":"","copyrightText":"","allowGeometryUpdates":true,"units":"esriMeters","size":9076736,"syncEnabled":false,"editorTrackingInfo":{"enableEditorTracking":false,"enableOwnershipAccessControl":false,"allowOthersToUpdate":true,"allowOthersToDelete":true},"xssPreventionInfo":{"xssPreventionEnabled":true,"xssPreventionRule":"InputOnly","xssInputRule":"rejectInvalid"},"tables":[],"_ssl":false,"name":"test2333"}
	# targetType:featureService
	# f:json
	# token:5x_BZvtUemIkI1RStEK0H6jEjDKRtUTbmsJjKlCbmUIFaQwEiR4JYS54YVZ1ySEPV2S4w7Uli8WbvMQgTGiGn2cnnE-RARMVlZDB2NWLKdPKpbhLEjmUpVgAbkYeBN-TSHf-CCUqKuqastIQPVHviIIIIAcGY786whMd88g4TVElTos9XFSte-3Nn55FPr_0
	
	create_params = {}

	# =================================================================
	# Add To Definition POST
	# =================================================================
	# http://services.arcgis.com/EDxZDh4HqQ1a9KvA/arcgis/rest/admin/services/test2333/FeatureServer/AddToDefinition
	# addToDefinition:{"layers":[{"currentVersion":10.21,"id":0,"name":"BRA_adm0","type":"Feature Layer","displayField":"","description":"","copyrightText":"","defaultVisibility":true,"editingInfo":{"lastEditDate":null},"relationships":[],"isDataVersioned":false,"supportsCalculate":true,"supportsAttachmentsByUploadId":true,"supportsRollbackOnFailureParameter":true,"supportsStatistics":true,"supportsAdvancedQueries":true,"advancedQueryCapabilities":{"supportsPagination":true,"supportsQueryWithDistance":true,"supportsReturningQueryExtent":true,"supportsStatistics":true,"supportsOrderBy":true,"supportsDistinct":true},"geometryType":"esriGeometryPolygon","minScale":0,"maxScale":0,"extent":{"xmin":14123994.352868726,"ymin":11732323.320242962,"xmax":-17634677.134016134,"ymax":17974424.219580036,"spatialReference":{"wkid":102100}},"drawingInfo":{"renderer":{"type":"simple","symbol":{"type":"esriSFS","style":"esriSFSSolid","color":[76,129,205,191],"outline":{"type":"esriSLS","style":"esriSLSSolid","color":[0,0,0,255],"width":0.75}},"label":"","description":""},"transparency":0,"labelingInfo":null},"allowGeometryUpdates":true,"hasAttachments":false,"htmlPopupType":"esriServerHTMLPopupTypeNone","hasM":false,"hasZ":false,"objectIdField":"FID","globalIdField":"","typeIdField":"","fields":[{"name":"FID","type":"esriFieldTypeInteger","actualType":"int","alias":"FID","sqlType":"sqlTypeInteger","nullable":false,"editable":false,"domain":null,"defaultValue":null},{"name":"ID_0","type":"esriFieldTypeInteger","actualType":"int","alias":"ID_0","sqlType":"sqlTypeInteger","nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"ISO","sqlType":"sqlTypeNVarchar","length":3,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ENGLI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ENGLI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ISO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ISO","sqlType":"sqlTypeNVarchar","length":54,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_FAO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_FAO","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_LOCAL","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_LOCAL","sqlType":"sqlTypeNVarchar","length":54,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_OBSOL","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_OBSOL","sqlType":"sqlTypeNVarchar","length":150,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_VARIA","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_VARIA","sqlType":"sqlTypeNVarchar","length":160,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_NONLA","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_NONLA","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_FRENC","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_FRENC","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_SPANI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_SPANI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_RUSSI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_RUSSI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_ARABI","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_ARABI","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"NAME_CHINE","type":"esriFieldTypeString","actualType":"nvarchar","alias":"NAME_CHINE","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"WASPARTOF","type":"esriFieldTypeString","actualType":"nvarchar","alias":"WASPARTOF","sqlType":"sqlTypeNVarchar","length":100,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"CONTAINS_","type":"esriFieldTypeString","actualType":"nvarchar","alias":"CONTAINS","sqlType":"sqlTypeNVarchar","length":50,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"SOVEREIGN","type":"esriFieldTypeString","actualType":"nvarchar","alias":"SOVEREIGN","sqlType":"sqlTypeNVarchar","length":40,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISO2","type":"esriFieldTypeString","actualType":"nvarchar","alias":"ISO2","sqlType":"sqlTypeNVarchar","length":4,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"WWW","type":"esriFieldTypeString","actualType":"nvarchar","alias":"WWW","sqlType":"sqlTypeNVarchar","length":2,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"FIPS","type":"esriFieldTypeString","actualType":"nvarchar","alias":"FIPS","sqlType":"sqlTypeNVarchar","length":6,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"ISON","type":"esriFieldTypeDouble","actualType":"float","alias":"ISON","sqlType":"sqlTypeFloat","nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"VALIDFR","type":"esriFieldTypeString","actualType":"nvarchar","alias":"VALIDFR","sqlType":"sqlTypeNVarchar","length":12,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"VALIDTO","type":"esriFieldTypeString","actualType":"nvarchar","alias":"VALIDTO","sqlType":"sqlTypeNVarchar","length":10,"nullable":true,"editable":true,"domain":null,"defaultValue":null},{"name":"EUmember","type":"esriFieldTypeDouble","actualType":"float","alias":"EUmember","sqlType":"sqlTypeFloat","nullable":true,"editable":true,"domain":null,"defaultValue":null}],"types":[],"templates":[{"name":"New Feature","description":"","drawingTool":"esriFeatureEditToolPolygon","prototype":{"attributes":{"ID_0":null,"ISO":null,"NAME_ENGLI":null,"NAME_ISO":null,"NAME_FAO":null,"NAME_LOCAL":null,"NAME_OBSOL":null,"NAME_VARIA":null,"NAME_NONLA":null,"NAME_FRENC":null,"NAME_SPANI":null,"NAME_RUSSI":null,"NAME_ARABI":null,"NAME_CHINE":null,"WASPARTOF":null,"CONTAINS_":null,"SOVEREIGN":null,"ISO2":null,"WWW":null,"FIPS":null,"ISON":null,"VALIDFR":null,"VALIDTO":null,"EUmember":null}}}],"supportedQueryFormats":"JSON","hasStaticData":false,"maxRecordCount":2000,"capabilities":"Query,Editing,Create,Update,Delete","adminLayerInfo":{"geometryField":{"name":"Shape","srid":102100}}}]}
	# f:json
	# token:5x_BZvtUemIkI1RStEK0H6jEjDKRtUTbmsJjKlCbmUIFaQwEiR4JYS54YVZ1ySEPV2S4w7Uli8WbvMQgTGiGn2cnnE-RARMVlZDB2NWLKdPKpbhLEjmUpVgAbkYeBN-TSHf-CCUqKuqastIQPVHviIIIIAcGY786whMd88g4TVElTos9XFSte-3Nn55FPr_0

	# =================================================================
	#Update Service POST
	# =================================================================

	# title:test2333
	# description:
	# tags:gadm
	# extent:126.878,71.942,-158.415,83.165
	# thumbnailURL:http://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/export?size=200,133&bboxSR=4326&format=png24&f=image&bbox=126.878,71.942,-158.415,83.165
	# typeKeywords:ArcGIS Server,Data,Feature Access,Feature Service,Service,Hosted Service
	# f:json
	# token:5x_BZvtUemIkI1RStEK0H6jEjDKRtUTbmsJjKlCbmUIFaQwEiR4JYS54YVZ1ySEPV2S4w7Uli8WbvMQgTGiGn2cnnE-RARMVlZDB2NWLKdPKpbhLEjmUpVgAbkYeBN-TSHf-CCUqKuqastIQPVHviIIIIAcGY786whMd88g4TVElTos9XFSte-3Nn55FPr_0
def get_user_content(username, token):
	users_url = "https://www.arcgis.com/sharing/rest/content/users/{}".format(username)
	params = {}
	params['f'] = 'json'
	params['token'] = token

	r = requests.get(users_url, params=params)
	r.raise_for_status()
	if not r.json()['items']:
		raise Exception('FAILED >> AGOL CONTENT REQUEST : {}'.format(users_url))
	return  pandas.DataFrame(r.json()['items'])

def delete_item(username, itemId, token):
	delete_item_url = "https://www.arcgis.com/sharing/rest/content/users/{}/items/{}/delete".format(username, itemId)
	delete_item_url = contentRoot + settings.agol_user + "/items/" + itemId + "/delete"
	params = {}
	params['f'] = 'json'
	params['token'] = token
	r = requests.post(deleteUrl, data=params)
	r.raise_for_status()
	if 'success' not in r.json().keys() or not r.json()['success']:
		raise KeyError('FAILED >> AGOL DELETE ITEM : {} : {}'.format(itemId), r.json())
	if r.json()['success']:
		print('Successfully Deleted Item {} : {}').format(itemId, r.json())
	return r.json()

def add_features(url, features, token):
	add_url = url + '/addFeatures'
	params = {}
	params['features'] = json.dumps(features, encoding='latin-1')
	params['f'] = 'json'
	params['token'] = token
	r = requests.post(add_url, data=params)
	
	if 'addResults' not in r.json().keys():
		print r.json()
		raise KeyError('addResults field missing')

	if r.json():
		print r.json()
		print 'added {} items to {}'.format(len(r.json()['addResults']), url)

def update_features(url, features, token):
	if isinstance(features, pandas.DataFrame):
		json_string = dataframe_to_featureset(dataframe)
	elif isinstance(features, list):
		json_string = json.dumps(features, encoding='latin-1')
	
	update_url = url + '/updateFeatures'
	params = {}
	params['Features'] = json_string
	params['f'] = 'json'
	params['token'] = token
	r = requests.post(update_url, data=params)
	r.raise_for_status()
	if r.json():
		print 'updated {} items from {}'.format(len(r.json()['updateResults']), url)

def delete_features(url, where, token):
	delete_url = url + '/deleteFeatures'
	params = {}
	params['where'] = where
	params['f'] = 'json'
	params['token'] = token
	r = requests.post(delete_url, data=params)
	r.raise_for_status()
	if r.json():
		print 'deleted {} items from {}'.format(len(r.json()['deleteResults']), url)

def query_to_dataframe(layer, where, token=None, outFields='*', chunkSize=100, useAliases=True):
	featureset = query_layer(layer, where, token, outFields, chunkSize)
	return featureset_to_dataframe(featureset, useAliases=useAliases)

def query_layer(layer, where, token=None, outFields='*', chunkSize=100, returnGeometry=False):
	url = layer + r'/query'

	params = {}
	params['where'] = where
	params['outFields'] = outFields
	params['returnGeometry'] = returnGeometry
	params['token'] = token
	params['f'] = 'json'
	params['returnIdsOnly'] = True

	ids_req = requests.post(url, data=params)
	ids_req.raise_for_status()
	oid_field_name = ids_req.json().get('objectIdFieldName')
	ids_response = ids_req.json().get('objectIds')
	params['returnIdsOnly'] = False
	params['where'] = ''

	featureset = None
	for ids in chunker(ids_response, chunkSize):
		params['objectIds'] = ','.join(map(str, ids))
		req = requests.post(url, data=params)
		req.raise_for_status()
		feat_response = req.json()
		if not featureset:
			featureset = feat_response
		else:
			featureset['features'] += feat_response['features']
	if not featureset:
		featureset = {}
		featureset['features'] = []

	return featureset
	



