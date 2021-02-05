
from enum import Enum
import logging

class FB_Endpoints:
    fb_api_version = 'v3.3'
    create_event_set_url = "https://graph.facebook.com/{api_version}/{business_id}/offline_conversion_data_sets"
    attach_event_set_url = "https://graph.facebook.com/{api_version}/{event_set_id}/adaccounts"
    upload_url = "https://graph.facebook.com/{api_version}/{event_set_id}/events"
    upload_status_url = "https://graph.facebook.com/{api_version}/{event_set_id}/uploads"
    request_run_url = "https://graph.facebook.com/{api_version}/{business_id}/upload_event"
    request_run_status_url = "https://graph.facebook.com/{api_version}/{request_id}"
    download_report_url = "https://graph.facebook.com/{api_version}/{business_id}/measurement_reports"

    @classmethod
    def get_url(cls, api = None, api_version = fb_api_version, **kwargs) -> str:
        return api.value.format(api_version = api_version, **kwargs)

class API(Enum):
    CREATE_EVENT_SET = FB_Endpoints.create_event_set_url
    ATTACH_EVENT_SET = FB_Endpoints.attach_event_set_url
    UPLOAD = FB_Endpoints.upload_url
    UPLOAD_STATUS = FB_Endpoints.upload_status_url
    UPLOAD_EVALUTION = FB_Endpoints.request_run_url
    UPLOAD_EVALUTION_STATUS = FB_Endpoints.request_run_status_url
    DOWNLOAD_REPORT_URLS = FB_Endpoints.download_report_url
    