import os

class Taxonomy_Grp:
    
    def __init__(self, tg_name, key_cols=[], target_cols=[], data_location=''):
        self.tg_name = tg_name
        self.key_cols = key_cols
        self.target_cols = target_cols
        self.location =os.path.join(data_location, tg_name)
        
    def get_dict(self):
        if len(self.target_cols) == 0:
            return {'tg_name': self.tg_name}
        
        return {'tg_name': self.tg_name, 
                'key_cols': self.key_cols, 
                'target_cols': self.target_cols, 
                'location': self.location}

    def __str__(self):
        if len(self.target_cols) == 0:
            return 'tg_name: {}'.format(self.tg_name)
        return 'tg_name: {}, key_cols: {}, target_cols: {}, location: {}'.format(self.tg_name, self.key_cols, self.target_cols,self.location)

