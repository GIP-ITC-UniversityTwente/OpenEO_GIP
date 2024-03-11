import common

class CustomException(Exception):
    def __init__(self, code, job_id, parameter, message):
        self.jsonErr = common.errorJson(code, job_id, message)
        idx = self.jsonErr['message'].find('{parameter}')
        if idx != -1:
            s = self.jsonErr['message'].replace('{parameter}', parameter)
            self.jsonErr['message'] = s
        idx = self.jsonErr['message'].find('{process}') 
        if idx != -1:
            s = self.jsonErr['message'].replace('{process}', job_id)
            self.jsonErr['message'] = s
        idx = self.jsonErr['message'].find('{reason}') 
        if idx != -1:
            s = self.jsonErr['message'].replace('{reason}', message)
            self.jsonErr['message'] = s

        super().__init__(message)