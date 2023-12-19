#!/usr/bin/env python
# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from json import JSONDecodeError

import requests
from cached_property import cached_property
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry


class HALNotReadyError(Exception):
    pass


class HALCommunicator(AppLogger):
    """
    This class helps navigate through REST API that uses the HAL standard.
    """
    acceptable_code = [200, 201, 204]

    def __init__(self, auth_url, bsd_url, username, password):
        self.auth_url = auth_url
        self.bsd_url = bsd_url
        self.username = username
        self.password = password

    def _validate_response(self, response):
        """Check that the response has an acceptable code and raise if it does not"""
        if response.status_code not in self.acceptable_code:
            self.error(response.request.method + ': ' + response.request.url + " with " + str(response.request.body))
            self.error("headers: {}".format(response.request.headers))
            self.error("<{}>: {}".format(response.status_code, response.text))
            raise ValueError('The HTTP status code ({}) is not one of the acceptable codes ({})'.format(
                str(response.status_code), str(self.acceptable_code))
            )
        return response

    @cached_property
    def token(self):
        """Retrieve the token from the AAP REST API then cache it for further quering"""
        response = requests.get(self.auth_url, auth=(self.username, self.password))
        self._validate_response(response)
        return response.text

    @retry(exceptions=(ValueError, requests.RequestException), tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _req(self, method, url, **kwargs):
        """private method that sends a request using the specified method. It adds the headers required by bsd"""
        headers = kwargs.pop('headers', {})
        headers.update({'Accept': 'application/hal+json', 'Authorization': 'Bearer ' + self.token})
        if 'json' in kwargs:
            headers['Content-Type'] = 'application/json'
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            **kwargs
        )
        self._validate_response(response)
        return response

    def follows(self, query, json_obj=None, method='GET', url_template_values=None, join_url=None, **kwargs):
        """
        Finds a link within the json_obj using a query string or list, modify the link using the
        url_template_values dictionary then query the link using the method and any additional keyword argument.
        If the json_obj is not specified then it will use the root query defined by the base url.
        """
        all_pages = kwargs.pop('all_pages', False)

        if json_obj is None:
            json_obj = self.root
        # Drill down into a dict using dot notation
        _json_obj = json_obj
        if isinstance(query, str):
            query_list = query.split('.')
        else:
            query_list = query
        for query_element in query_list:
            if query_element in _json_obj:
                _json_obj = _json_obj[query_element]
            else:
                raise KeyError('{} does not exist in json object'.format(query_element, _json_obj))
        if not isinstance(_json_obj, str):
            raise ValueError('The result of the query_string must be a string to use as a url')
        url = _json_obj
        # replace the template in the url with the value provided
        if url_template_values:
            for k, v in url_template_values.items():
                url = re.sub('{(' + k + ')(:.*)?}', v, url)
        if join_url:
            url += '/' + join_url
        # Now query the url
        response = self._req(method, url, **kwargs)
        try:
            json_response = response.json()
        except JSONDecodeError:
            self.debug(f'No response available for request {method} to {url}')
            return {}

        # Depaginate the call if requested
        if all_pages is True:
            # This depagination code will iterate over all the pages available until the pages comes back  without a
            # next page. It stores the embedded elements in the initial query's json response
            content = json_response
            while 'next' in content.get('_links'):
                content = self._req(method, content.get('_links').get('next').get('href'), **kwargs).json()
                for key in content.get('_embedded'):
                    json_response['_embedded'][key].extend(content.get('_embedded').get(key))
            # Remove the pagination information as it is not relevant to the depaginated response
            if 'page' in json_response: json_response.pop('page')
            if 'first' in json_response['_links']: json_response['_links'].pop('first')
            if 'last' in json_response['_links']: json_response['_links'].pop('last')
            if 'next' in json_response['_links']: json_response['_links'].pop('next')
        return json_response

    def follows_link(self, key, json_obj=None, method='GET', url_template_values=None, join_url=None, **kwargs):
        """
        Same function as follows but construct the query_string from a single keyword surrounded by '_links' and 'href'.
        """
        return self.follows(('_links', key, 'href'),
                            json_obj=json_obj, method=method, url_template_values=url_template_values,
                            join_url=join_url, **kwargs)

    @cached_property
    def root(self):
        return self._req('GET', self.bsd_url).json()

    @property
    def communicator_attributes(self):
        raise NotImplementedError


class AAPHALCommunicator(HALCommunicator):

    def __init__(self, auth_url, bsd_url, username, password, domain=None):
        super(AAPHALCommunicator, self).__init__(auth_url, bsd_url, username, password)
        self.domain = domain

    @property
    def communicator_attributes(self):
        return {'domain': self.domain}


class WebinHALCommunicator(HALCommunicator):

    @cached_property
    def token(self):
        """Retrieve the token from the ENA Webin REST API then cache it for further quering"""
        response = requests.post(self.auth_url,
                                 json={"authRealms": ["ENA"], "password": self.password,
                                       "username": self.username})
        self._validate_response(response)
        return response.text

    @property
    def communicator_attributes(self):
        return {'webinSubmissionAccountId': self.username}
