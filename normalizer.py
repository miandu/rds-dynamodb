#print!/usr/bin/python3
import json,logging,sys,os
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
import utils

class Normalizer():
    ## Normalizer class hold data and configurations for normalizing source/target pairs
    ## source(input) of normalization
    source={}
    ## target(output) of normalization
    target={}
    ## mapping from target key to source key or lambda function
    target_source_rule={}
    
    def set_source(self,source):
        self.source=source

    def set_target(self,target):
        self.target=target

    def get_source_value(self,s):
        ## get value from source with key s or lambda function s
        mapping=self.target_source_rule[s]
        if isinstance(mapping,str):
            ## if mapping is a string key
            return self.source.get(mapping)
        else:            
            ## if mapping is lambda function
            return mapping(self)


    def normalize_source_to_target(self,cf,source):
    ## Normalizing from source obect to target object
        self.set_source(source)
        if self.source:
            for s in self.target_source_rule:
                self.target[s] = self.get_source_value(s)
        else:
            print("No source specified")
