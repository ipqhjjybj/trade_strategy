# coding=utf-8

import sys
import traceback
from datetime import datetime
from enum import Enum
from multiprocessing.dummy import Pool
from threading import Lock
import requests


class RequestStatus(Enum):
    ready = 0  # Request created
    success = 1  # Request successful (status code 2xx)
    failed = 2  # Request failed (status code not 2xx)
    error = 3  # Exception raised


pool = Pool(20)


class Request(object):
    """
    Request object for status check.
    """

    def __init__(self, method, path, params, data, headers, callback=None, on_failed=None, on_error=None, extra=None):
        self.method = method
        self.path = path
        self.callback = callback
        self.params = params
        self.data = data
        self.headers = headers

        self.on_failed = on_failed
        self.on_error = on_error
        self.extra = extra

        self.response = None
        self.status = RequestStatus.ready

    def simple_str(self):
        return "method:{},path:{},params:{},data:{}".format(self.method, self.path, self.params, self.data)

    def __str__(self):
        if self.response is None:
            status_code = 404
        else:
            status_code = self.response.status_code

        return (
            "request : {} {} {} because {}: \n"
            "headers: {}\n"
            "params: {}\n"
            "data: {}\n"
            "response:"
            "{}\n".format(
                self.method,
                self.path,
                self.status.name,
                status_code,
                self.headers,
                self.params,
                self.data,
                "" if self.response is None else self.response.text,
            )
        )


class RestClient(object):
    """
    HTTP Client designed for all sorts of trading RESTFul API.

    * Reimplement sign function to add signature function.
    * Reimplement on_failed function to handle Non-2xx responses.
    * Use on_failed parameter in add_request function for individual Non-2xx response handling.
    * Reimplement on_error function to handle exception msg.
    """

    class Session:

        def __init__(self, client, session):
            self.client = client
            self.session = session

        def __enter__(self):
            return self.session

        def __exit__(self, exc_type, exc_val, exc_tb):
            with self.client._sessions_lock:
                self.client._sessions.append(self.session)

    def __init__(self):
        self.url_base = ''  # type: str
        self._active = False

        self.proxies = None

        self._tasks_lock = Lock()
        self._tasks = []
        self._sessions_lock = Lock()
        self._sessions = []

    def init(self, url_base, proxy_host="", proxy_port=0):
        """
        Init rest client with url_base which is the API root address.
        e.g. 'https://www.bitmex.com/api/v1/'
        """
        self.url_base = url_base

        if proxy_host and proxy_port:
            proxy = "{}:{}".format(proxy_host, proxy_port)
            self.proxies = {"http": proxy, "https": proxy}

    def _create_session(self):
        return requests.session()

    def start(self):
        """
        Start rest client .
        """
        if self._active:
            return
        self._active = True

    def stop(self):
        """
        Stop rest client immediately.
        """
        self._active = False

    def join(self):
        """
        Wait till all requests are processed.
        """
        for task in self._tasks:
            task.wait()

    def _process_requst_finished(self, request, response):
        request.response = response
        status_code = response.status_code

        if status_code // 100 == 2:  # 2xx codes are all successful
            if status_code == 204:
                json_body = None
            else:
                json_body = response.json()

            request.callback(json_body, request)
            request.status = RequestStatus.success
        else:
            request.status = RequestStatus.failed

            if request.on_failed:
                request.on_failed(status_code, request)
            else:
                self.on_failed(status_code, request)

    def _process_request(self, request):
        """
        Sending request to server and get result.
        """
        try:
            with self._get_session() as session:
                request = self.sign(request)

                url = self.make_full_url(request.path)
                response = session.request(
                    request.method,
                    url,
                    headers=request.headers,
                    params=request.params,
                    data=request.data,
                    proxies=self.proxies,
                    timeout=15
                    # verify=False
                )
                self._process_requst_finished(request, response)
        except Exception:
            request.status = RequestStatus.error
            t, v, tb = sys.exc_info()
            if request.on_error:
                request.on_error(t, v, tb, request)
            else:
                self.on_error(t, v, tb, request)

    def direct_request(self, method, path, callback, params=None, data=None, headers=None, on_failed=None,
                       on_error=None,
                       extra=None):
        try:
            request = Request(method=method, path=path, params=params, data=data, headers=headers,
                              callback=callback, on_failed=on_failed, on_error=on_error, extra=extra)
            request = self.sign(request)
            url = self.make_full_url(request.path)

            response = requests.request(
                request.method,
                url,
                headers=request.headers,
                params=request.params,
                data=request.data,
                proxies=self.proxies,
                timeout=15
                # verify=False
            )
            self._process_requst_finished(request, response)
            return request
        except Exception as ex:
            print("[direct_request] ex:{}".format(ex))

    def add_request(self, method, path, callback, params=None, data=None, headers=None, on_failed=None, on_error=None,
                    extra=None):
        """
        Add a new request.
        :param method: GET, POST, PUT, DELETE, QUERY
        :param path:
        :param callback: callback function if 2xx status, type: (dict, Request)
        :param params: dict for query string
        :param data: Http body. If it is a dict, it will be converted to form-data. Otherwise, it will be converted to bytes.
        :param headers: dict for headers
        :param on_failed: callback function if Non-2xx status, type, type: (code, dict, Request)
        :param on_error: callback function when catching Python exception, type: (etype, evalue, tb, Request)
        :param extra: Any extra data which can be used when handling callback
        :return: Request
        """
        request = Request(method=method, path=path, params=params, data=data, headers=headers,
                          callback=callback, on_failed=on_failed, on_error=on_error, extra=extra)

        task = pool.apply_async(
            self._process_request,
            args=[request, ],
            callback=self._clean_finished_tasks,
            # error_callback=lambda e: self.on_error(type(e), e, e.__traceback__, request),
        )
        self._push_task(task)
        return request

    def _push_task(self, task):
        with self._tasks_lock:
            self._tasks.append(task)

    def _clean_finished_tasks(self, result):
        with self._tasks_lock:
            not_finished_tasks = [i for i in self._tasks if not i.ready()]
            self._tasks = not_finished_tasks

    def _get_session(self):
        with self._sessions_lock:
            if self._sessions:
                return self.Session(self, self._sessions.pop())
            else:
                return self.Session(self, self._create_session())

    def sign(self, request):
        """
        This function is called before sending any request out.
        Please implement signature method here.
        @:return (request)
        """
        return request

    def on_failed(self, status_code, request):
        """
        Default on_failed handler for Non-2xx response.
        """
        sys.stderr.write('request:{},status_code:{}'.format(str(request), status_code))

    def on_error(self, exception_type, exception_value, tb, request):
        """
        Default on_error handler for Python exception.
        """
        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )
        sys.excepthook(exception_type, exception_value, tb)

    def exception_detail(self, exception_type, exception_value, tb, request):
        text = "[{}]: Unhandled RestClient Error:{}\n".format(
            datetime.now().isoformat(), exception_type
        )
        text += "request:{}\n".format(request)
        text += "Exception trace: \n"
        text += "".join(
            traceback.format_exception(exception_type, exception_value, tb)
        )
        return text

    def make_full_url(self, path):
        """
        Make relative api path into full url.
        eg: make_full_url('/get') == 'http://xxxxx/get'
        """
        url = self.url_base + path
        return url

    def request(self, method, path, params=None, data=None, headers=None):
        """
        Add a new request.
        :param method: GET, POST, PUT, DELETE, QUERY
        :param path:
        :param params: dict for query string
        :param data: dict for body
        :param headers: dict for headers
        :return: requests.Response
        """
        request = Request(method, path, params, data, headers)
        request = self.sign(request)

        url = self.make_full_url(request.path)

        response = requests.request(
            request.method,
            url,
            headers=request.headers,
            params=request.params,
            data=request.data,
            proxies=self.proxies,
            timeout=15
            # verify=False
        )

        return response

    def wrap_request(self, method, path, params=None, data=None, headers=None):
        try:
            response = self.request(method, path, params, data, headers)
            return response.json()
        except Exception as ex:
            print(ex)
            pass
