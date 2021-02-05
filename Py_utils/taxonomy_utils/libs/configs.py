from lxml import etree

class Config:

    '''
        qubole_config from platform config xml
    '''
    @classmethod
    def get_qubole_config(cls, config):
            tree = etree.parse(config)
            platform = tree.xpath("/configroot/set[name='PLATFORM']/stringval")[0].text.lower()
            if platform == "qubole":
                fields = ['s3_location', 'token', 'url', 'cluster',
                        'access_key', 'secret_key', 'region']
            else:
                return "errored Qubole should be enable!!"
            result = {}
            for field in fields:
                result[field] = tree.xpath(
                    "//{}_settings/set[name='{}']/stringval".format(platform, field)
                )[0].text.strip()
            return result