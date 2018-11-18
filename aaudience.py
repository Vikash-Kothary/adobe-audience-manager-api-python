# -*- coding: utf-8 -*-
"""
@author: julienpiccini
"""
import base64 as _base64
import requests as _requests
import pandas as _pd
import json as _json
from pathlib import Path as _Path
import time as _time
import re as _re
_c_path = _Path.cwd() #get the current folder
_new_path = _c_path.joinpath('aaudience') #create new folder
_new_path.mkdir(exist_ok=True) #create a new folder to store the data

_clientID = ""
_clientSecret = ""
_partnerName = ""
_username = ""
_password = ''

_base_endpoint = "https://api.demdex.com/v1/"


def importAccess(file):
    """This method enables you to import the aplication ID, the secret of the application and the reportsuite to be used.
    Parameters : 
        file : REQUIRED : path to the file to upload the credential information
    """
    import re
    clientID = re.compile('clientID.+"(.+?)"')
    clientSecret = re.compile('clientSecret.+"(.+?)"')
    partnerName = re.compile('partnerName.+"(.+?)"')
    username = re.compile('username.+"(.+?)"')
    password = re.compile('password.+"(.+?)"')
    with open(file,'r') as f :
        for line in f:
            line = line.replace('\'','"') 
            if clientID.search(line): 
                cid = clientID.search(line).group(1)
            if clientSecret.search(line):
                cs = clientSecret.search(line).group(1)
            if partnerName.search(line):
                pn = partnerName.search(line).group(1)
            if username.search(line):
                un = username.search(line).group(1)
            if password.search(line):
                pw = password.search(line).group(1)
    if cs and cid and pn and un and pw:
        global _clientID 
        _clientID = cid
        global _clientSecret 
        _clientSecret= cs
        global _partnerName 
        _partnerName= pn
        global _username
        _username= un
        global _password
        _password = pw
    else :
        print('one of the element is missing')
    

def __getToken(clientID,clientSecret,username,password):
    """ Function to create the token for the Audience API.
    
    Based on the Client ID, Client Secret, username and password. 
    """
    oauth_request = "https://api.demdex.com/oauth/token"
    combi =  clientID+':'+clientSecret
    b64combi = _base64.b64encode(combi.encode())
    base64IDSecret = b64combi.decode()
    header = {'Authorization' : 'Basic '+base64IDSecret,'Content-Type': 'application/x-www-form-urlencoded'}
    body = {'grant_type':'password','username':username,'password':password}
    audience_request = _requests.post(oauth_request,headers=header,data=body)
    token = audience_request.json()['access_token']
    return token


def __newfilename(partnerName):
    fmonth = _time.strftime("%m")
    fday = _time.strftime("%d")
    filename=_new_path.as_posix()+'/'+'report_aam_'+partnerName+'_'+fmonth+'_'+fday
    return filename

def __loop_folders(obj,ids=[],names=[],parentids=[]):
    """Loop function to retrieve ParentFolderID and FolderID 
    """
    ids = ids
    names=names
    parentids=parentids
    if type(obj) == list :
        for o in obj:
            __loop_folders(o,ids=ids,names=names,parentids=parentids)##loop the loop
    if type(obj) == dict :
        if 'folderId' in obj.keys():
            ids.append(obj['folderId'])##retrieve mother id
            names.append(obj['name'])##retrieve mother id
            parentids.append(obj['parentFolderId'])
        if 'subFolders' in obj.keys():
            __loop_folders(obj['subFolders'],ids=ids,names=names,parentids=parentids)##loop the loop
    return ids, names, parentids
        
def _getTraitsFolders(token):
    "Function to retrieve the Traits Folders"
    trait_folders = "folders/traits/"
    endpoint_folder = _base_endpoint+trait_folders
    header =  {'Authorization' : 'Bearer '+token}
    all_traits_folders = _requests.get(endpoint_folder,headers=header)
    folders_list = all_traits_folders.json()
    ids,names,parentids = __loop_folders(folders_list)
    dict_dataFrame = {
            'folderId' : ids,
            'name' : names,
            'parentFolderId' : parentids
            }
    df_folders = _pd.DataFrame(dict_dataFrame)
    return df_folders

def _getTraits(token):
    "Function to retrieve the Traits "
    traits = 'traits/'
    endpoint_folder = _base_endpoint+traits
    header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json'}
    data = {'includeDetails':'true'}
    all_traits = _requests.get(endpoint_folder,headers=header,params=data)
    df_traits= _pd.DataFrame(all_traits.json())
    order_columns = ['sid','name','description','integrationCode','dataSourceId','pid','folderId','status','traitRule','traitRuleVersion','traitType','updateTime','createTime','url']
    df_traits = df_traits[order_columns]##order the column by importance & remove crUID, backfillStatus,upUID,ttl
    return df_traits


def _getDataSource(token):
    "Function to retrieve the data sources"
    data_source = 'datasources/'
    endpoint_ds = _base_endpoint+data_source
    header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json'}
    data = {'includeThirdParty':'true'}
    all_ds = _requests.get(endpoint_ds,headers=header,params=data)
    df_all_ds = _pd.DataFrame(all_ds.json())
    selected_columns = ['dataSourceId','name','description','pid','integrationCode','status','masterDataSourceIdProvider','containerIds','inboundS2S','uniqueSegmentIntegrationCodes','uniqueTraitIntegrationCodes','useAudienceManagerVisitorID','createTime']
    export_columns = ['dataExportRestrictions','allowDataSharing','allowDeviceGraphSharing','outboundS2S']
    full_cols_restricted = selected_columns + export_columns
    df_lim_ds = df_all_ds[full_cols_restricted]
    return df_lim_ds

def _getSegments(token,incldueInstant=True,includePrediction=True):
    "Function to retrieve the segments"
    segments = 'segments/'
    endpoint_segments = _base_endpoint+segments
    header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json'}
    data = {'includeTraitDataSourceIds':'true','includeMetrics':'true','includeAddressableAudienceMetrics':'true'}
    all_segments = _requests.get(endpoint_segments,headers=header,params=data)
    df_seg_all = _pd.DataFrame(all_segments.json())
    selected_columns = ['sid','name','description','segmentRule','folderId','integrationCode','status','updateTime','createTime','dataSourceId','traitDataSourceIds','pid']
    instant_cols = ['instantUniques1Day','instantUniques7Day','instantUniques14Day','instantUniques30Day','instantUniques60Day','instantUniques90Day','instantUniquesLifetime']
    prediction_cols= ['totalUniques1Day','totalUniques7Day','totalUniques14Day','totalUniques30Day','totalUniques60Day','totalUniques90Day','totalUniquesLifetime']
    if incldueInstant:
        selected_columns += instant_cols
    if includePrediction :
        selected_columns += prediction_cols
    df_segment = df_seg_all[selected_columns]
    return df_segment

def _getSegmentsFolders(token):
    "Function to retrieve the segments Folders"
    segmentsFolders = 'folders/segments/'
    endpoint_segmentsFolders = _base_endpoint+segmentsFolders
    header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json'}
    segmentsFolders_list = _requests.get(endpoint_segmentsFolders,headers=header)
    ids,names,parentids = __loop_folders(segmentsFolders_list)
    dict_dataFrame = {
            'folderId' : ids,
            'name' : names,
            'parentFolderId' : parentids
            }
    df_folders = _pd.DataFrame(dict_dataFrame)
    return df_folders

def _getDestinations(token):
    "Function to retrieve the destinations Folders"
    destinations = 'destinations/'
    endpoint_destinations = _base_endpoint+destinations
    header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json'}
    data = {'includeMetrics':'true','includeMasterDataSourceIdType':'true','includeAddressableAudienceMetrics':'true'}
    all_destinations = _requests.get(endpoint_destinations,headers=header,params=data)
    df_desti = _pd.DataFrame(all_destinations.json())
    return df_desti

def returnAudienceData(data='all'):
    """ 
    function that return dictionnary of DataFrame for the requested elements.
    The data are also stores in an excel in new folder call : "aaudience" 
    Possible arguments: 
        data : REQUIRED - Can take those values : 
            all : return Traits, Segments, Data Sources and Destinations information
            traits: return the traits & trait folders 
            segments: return the Segments & segments folders 
            datasources: return the data sources 
            destinations : return the destinations 
    """
    token = __getToken(_clientID,_clientSecret,_username,_password)
    filename = __newfilename(_partnerName)
    writer = _pd.ExcelWriter(filename+'.xlsx', engine='xlsxwriter')
    dict_data = dict()
    if data == 'all':
        df_traits = _getTraits(token)
        dict_data['Traits']=df_traits
        df_traitsFolders = _getTraitsFolders(token)
        dict_data['TraitsFolders']=df_traitsFolders
        df_segments = _getSegments(token,incldueInstant=True,includePrediction=True)
        dict_data['Segments'] = df_segments
        df_segmentsFolders = _getSegmentsFolders(token)
        dict_data['SegmentsFolders'] = df_segmentsFolders
        df_datasources = _getDataSource(token)
        dict_data['Datasources'] = df_datasources
        df_destinations = _getDestinations(token)
        dict_data['Destinations'] = df_destinations
        ##writing in Excel
        df_traits.to_excel(writer, sheet_name='traits',index=False)
        df_traitsFolders.to_excel(writer, sheet_name='traitsFolders',index=False)
        df_segments.to_excel(writer, sheet_name='segments',index=False)
        df_segmentsFolders.to_excel(writer, sheet_name='segmentFolders',index=False)
        df_datasources.to_excel(writer, sheet_name='datasources',index=False)
        df_destinations.to_excel(writer, sheet_name='destinations',index=False)
        writer.save()
    elif data=='segments':
        df_segments = _getSegments(token,incldueInstant=True,includePrediction=True)
        df_segmentsFolders = _getSegmentsFolders(token)
        df_segments.to_excel(writer, sheet_name='segments',index=False)
        df_segmentsFolders.to_excel(writer, sheet_name='segmentFolders')
        writer.save()
        dict_data['Segments'] = df_segments
        dict_data['SegmentsFolders'] = df_segmentsFolders
    elif data=='traits':
        df_traits = _getTraits(token)
        df_traitsFolders = _getTraitsFolders(token)
        df_traits.to_excel(writer, sheet_name='traits',index=False)
        df_traitsFolders.to_excel(writer, sheet_name='traitsFolders',index=False)
        writer.save()
        dict_data['TraitsFolders']=df_traitsFolders
        dict_data['Traits']=df_traits
    elif data=='datasources':
        df_datasources = _getDataSource(token)
        dict_data['Datasources'] = df_datasources
        df_datasources.to_excel(writer, sheet_name='datasources',index=False)
        writer.save()
    elif data == 'destinations':
        df_destinations = _getDestinations(token)
        dict_data['Destinations'] = df_destinations
        df_destinations.to_excel(writer, sheet_name='destinations',index=False)
        writer.save()
    return dict_data

def _putTraits(token,data,verbose=False):
    "Function to update the traits. Arguments : token, data (dataframe with traits to be updated)"
    response_data = []
    data.fillna('',inplace=True)
#    nb_data = len(data)
    count = 0
    for _, trait in data.iterrows():
        count+=1
        sid = trait['sid']
        try:
            dict_data = {x : y for (x, y) in trait.iteritems() if y is not "" }
            del dict_data['sid']
            traits = 'traits/'+str(sid)
            endpoint_update = _base_endpoint+traits
            header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
            update_traits = _requests.put(endpoint_update,headers=header,data=_json.dumps(dict_data))
            if update_traits.reason == 'Unauthorized':
                print('update token')
                token = __getToken(_clientID,_clientSecret,_username,_password)
                header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
                update_traits = _requests.put(endpoint_update,headers=header,data=_json.dumps(dict_data))
            response_data.append([sid,update_traits.reason])
        except :
            response_data.append([sid,'issue reading your data'])
            print('error reading the data at line : '+str(count))
#            raise NameError(update_traits.reason)
        if verbose:
            if count%50==0:
                print(str(count)+' rows done')
    df = _pd.DataFrame(response_data)
    df.columns = ['sid','status']
    return df

def _postTraits(token,data,verbose=False):
    "Function to update the traits. Arguments : token, data (dataframe with traits to be updated)"
    response_data = []
    data.fillna('',inplace=True)
    data['traitRule'] = data['traitRule'].str.replace("'",'"')
    for _, trait in data.iterrows():
        try:
            dict_data = {x : y for (x, y) in trait.iteritems() if y is not "" }
            if 'sid' in dict_data.keys():
                del dict_data['sid']
            if 'name' not in dict_data.keys() or 'dataSourceId' not in dict_data.keys() or 'folderId' not in dict_data.keys() or 'traitType' not in dict_data.keys():
                print('missing Required element')
                response_data.append([dict_data['name'],'error'])
            traits = 'traits/'
            endpoint_create = _base_endpoint+traits
            header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
            create_traits = _requests.post(endpoint_create,headers=header,json=dict_data)
            response_data.append([dict_data['name'],create_traits.reason])
        except : 
            raise NameError('error in reading your data')
    df = _pd.DataFrame(response_data)
    df.columns = ['sid','status']
    return df

def importTraits(action,data,verbose=False):
    """
    This method will import the data and apply the modification on your account.
    It will return a list with the imported data and if the modification has been realized.
    Parameters : 
        actions : REQUIRED : 2 possibles actions :
            - 'create' : Will create the list of traits
            - 'update' : Will import the list of traits (work with sid)
        data : REQUIRED : dataframe of the traits you would like to create 
        verbose : OPTIONAL : will print comments when number of rows reached.
    """
    token = __getToken(_clientID,_clientSecret,_username,_password)
    if action == 'create':
        dfresult = _postTraits(token,data,verbose=verbose)
    elif action == 'update':
        dfresult = _putTraits(token,data,verbose=verbose)
    else:
        print('action is not recognized')
        dfresult = ''
    return dfresult

def _putSegments(token,data,verbose=False):
    "Function to update the Segments. Arguments : token, data (dataframe with segments to be updated)"
    response_data = []
    data.fillna('',inplace=True)
    count = 0
    for _, segment in data.iterrows():
        count+=1
        sid = segment['sid']
        print(sid)
        try:
            dict_data = {x : y for (x, y) in segment.iteritems() if y is not "" }
            del dict_data['sid']
            for key in list(dict_data.keys()): ##clean the data that need to be sent
                if 'instantUnique' in key or 'totalUniques' in key or key == 'traitDataSourceIds' :
                    del dict_data[key]
            segments = 'segments/'+str(sid)
            endpoint_update = _base_endpoint+segments
            header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
            update_segments = _requests.put(endpoint_update,headers=header,data=_json.dumps(dict_data))
            if update_segments.reason == 'Unauthorized':
                print('update token')
                token = __getToken(_clientID,_clientSecret,_username,_password)
                header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
                update_segments = _requests.put(endpoint_update,headers=header,data=_json.dumps(dict_data))
            print(response_data.text)
            response_data.append([sid,update_segments.reason])
        except :
            response_data.append([sid,'issue reading your data'])
            print('error reading the data at line : '+str(count))
#            raise NameError(update_traits.reason)
        if verbose:
            if count%50==0:
                print(str(count)+' rows done')
    df = _pd.DataFrame(response_data)
    df.columns = ['sid','status']

def _postSegments(token,data,verbose=False):
    "Function to update the traits. Arguments : token, data (dataframe with traits to be updated)"
    response_data = []
    data.fillna('',inplace=True)
    data['segmentRule'] = data['traitRule'].str.replace("'",'"')
    for _, segment in data.iterrows():
        try:
            dict_data = {x : y for (x, y) in segment.iteritems() if y is not "" }
            if 'sid' in dict_data.keys():
                del dict_data['sid']
            for key in list(dict_data.keys()): ##clean the data that need to be sent
                if 'instantUnique' in key or 'totalUniques' in key or key == 'traitDataSourceIds' :
                    del dict_data[key]
            if 'name' not in dict_data.keys() or 'dataSourceId' not in dict_data.keys() or 'folderId' not in dict_data.keys() :
                print('missing Required element')
                response_data.append([dict_data['name'],'error'])
            traits = 'segments/'
            endpoint_create = _base_endpoint+traits
            header =  {'Authorization' : 'Bearer '+token,'accept': 'application/json',"Content-Type": "application/json"}
            create_segments = _requests.post(endpoint_create,headers=header,json=dict_data)
            response_data.append([dict_data['name'],create_segments.reason])
        except : 
            raise NameError('error in reading your data')
    df = _pd.DataFrame(response_data)
    df.columns = ['sid','status']
    return df

def importSegments(action,data,verbose=False):
    """
    This method will import the data and apply the modification on your account.
    It will return a list with the imported data and if the modification has been realized.
    Parameters : 
        actions : REQUIRED : 2 possibles actions :
            - 'create' : Will create the list of traits
            - 'update' : Will import the list of traits (work with sid)
        data : REQUIRED : dataframe of the traits you would like to create 
        verbose : OPTIONAL : will print comments when number of rows reached.
    """
    token = __getToken(_clientID,_clientSecret,_username,_password)
    if action == 'create':
        dfresult = _postSegments(token,data,verbose=verbose)
    elif action == 'update':
        dfresult = _putSegments(token,data,verbose=verbose)
    else:
        print('action is not recognized')
        dfresult = ''
    return dfresult

class audienceManagerData:
    """ 
    Class method that enable you to manipulate the big amount of data returned from the getElements method.
    
    As it is a class, it will create an instance and this instance is the element that you are going to manipulate.
    Please always refers to your instance name. 
    ie : 
        my_audience = audienceManagerData('report_Adobe.xlsx')
        mytraitsSearch = my_audience.traitSearch(name='my trait')
    
    """
    _Template = {
            'traits':{
                'new' : {
                        'name':['REQUIRED'],
                        'dataSourceId':['REQUIRED'],
                        'folderId':['REQUIRED'],
                        'description':['OPTIONAL'],
                        'traitRule':['OPTIONAL'],
                        'integrationCode':['OPTIONAL']
                        },
                'remove' : {
                        "traits_ids":['list of sids']
                        },
                'update':{
                        'sid':['REQUIRED'],
                        'name':['OPTIONAL'],
                        'dataSourceId':['OPTIONAL'],
                        'folderId':['OPTIONAL'],
                        'description':['OPTIONAL'],
                        'traitRule':['OPTIONAL'],
                        'integrationCode':['OPTIONAL']
                        }
            },
            'segments':{
                'new' :{
                        'name':['REQUIRED'],
                        'dataSourceId': ['REQUIRED'],
                        'segmentRule':['REQUIRED'],
                        'mergeRuleDataSourceId':['REQUIRED'],
                        'folderId':['REQUIRED'],
                        'description':['OPTIONAL'],
                        'integrationCode':['OPTIONAL']
                        },
                'remove' : {
                        "segments_ids":['list of ids']
                        },
                'update':{
                        'sid' : ['REQUIRED'],
                        'name':['OPTIONAL'],
                        'dataSourceId':['OPTIONAL'],
                        'mergeRuleDataSourceId':['OPTIONAL'],
                        'integrationCode': ['OPTIONAL'],
                        'folderId':['OPTIONAL'],
                        'segmentRule':['OPTIONAL'],
                        'description':['OPTIONAL']
                        }
                    }
        }
    
    def __init__(self,data):
        print(data)
        if data is not None : 
            self.template = dict()
            self.data = dict()
        if '.xlsx' in data:
            try:
                self.data['Traits'] = _pd.read_excel(data,sheet_name='traits')
            except:
                pass
            try:
                self.data['TraitsFolders'] = _pd.read_excel(data,sheet_name='traitsFolders')
            except:
                pass
            try:
                self.data['Segments'] = _pd.read_excel(data,sheet_name='segments')
            except:
                pass
            try:
                self.data['SegmentsFolders'] = _pd.read_excel(data,sheet_name='segmentFolders')
            except:
                pass
            try:
                self.data['Datasources'] = _pd.read_excel(data,sheet_name='datasources')
            except:
                pass
            try:
                self.data['Destinations'] = _pd.read_excel(data,sheet_name='destinations')
            except:
                pass
        elif type(data) == dict:
            print('dict')
            if 'Traits' in data.keys():
                self.data['Traits'] = data['Traits']
            if 'TraitsFolders' in data.keys():
                self.data['TraitsFolders'] = data['TraitsFolders']
            if 'Segments' in data.keys():
                self.data['Segments'] = data['Segments']
            if 'SegmentsFolders' in data.keys():
                self.data['SegmentsFolders'] = data['SegmentsFolders']
            if 'Datasources' in data.keys():
                self.data['Datasources'] = data['Datasources']
            if 'Destinations' in data.keys():
                self.data['Destinations'] = data['Destinations']
        elif data == 'new' or data == 'New':
            self.template = dict()
            self.template['Traits'] = self._Template['traits']['new']
            self.template['Segments'] = self._Template['segments']['new']
        elif data == 'update' or data == 'Update':
            self.template['Traits'] = self._Template['traits']['updates']
            self.template['Segments'] = self._Template['segments']['updates']
        elif data == 'remove' or data == 'Remove':
            self.template['Traits'] = self._Template['traits']['remove']
            self.template['Segments'] = self._Template['segments']['remove']
            
    
    def traitSearch(self,name=None,sid=None,ic=None,rule=None,ds_id=None, ds_name=None,f_id=None,f_name=None):
        """
        This method will return the different traits that match the search criteria.
        parameters : 
            name : will search for the pattern you have used using ReGex
            sid : will try to match EXACTLY your sid
            ic : will try to match EXACTLY your integration code
            rule : will search for the pattern you have used using ReGex in the TraitRule
            ds_id : will try to match EXACTLY your data source id
            ds_name : REQUIRED datasource data table : will search for the pattern you have used using ReGex in the data source name
            f_id : will try to match EXACTLY your folder id
            f_name :  REQUIRED Traits Folder data table : will search for the pattern you have used using ReGex in the Folder name
        """
        if 'Traits' not in self.data.keys():
            raise NameError('No Trait Data has been imported')
        df_trait = self.data['Traits']
        df_trait.fillna('',inplace=True)
        if name is not None:
            df_traits_searched=df_trait[df_trait['name'].str.contains(name,flags=_re.IGNORECASE)]
            return df_traits_searched
        if sid is not None:
            try:
                sid = int(sid)
            except:
                sid = sid
            df_traits_searched=df_trait[df_trait['sid'] == sid]
            return df_traits_searched
        if ic is not None:
            df_traits_searched=df_trait[df_trait['integrationCode'] == ic]
            return df_traits_searched
        if rule is not None:
            df_traits_searched=df_trait[df_trait['traitRule'].str.contains(rule,flags=_re.IGNORECASE)]
            return df_traits_searched
        if ds_id is not None: 
            df_traits_searched=df_trait[df_trait['dataSourceId'] == int(ds_id)]
            return df_traits_searched
        if f_id is not None:
            df_traits_searched=df_trait[df_trait['folderId'] == int(f_id)]
            return df_traits_searched
        ###require additional data
        if ds_name is not None:
            if 'Datasources' not in self.data.keys():
                raise NameError('No Datasources Data has been imported')
            else:
                df_ds = self.data['Datasources'][['dataSourceId','name']].add_prefix('ds_')
            df_trait = df_trait.merge(df_ds,how='left',left_on='dataSourceId',right_on='ds_dataSourceId')
            del df_trait['ds_dataSourceId']
            df_traits_searched=df_trait[df_trait['ds_name'].contains(ds_name)]
            return df_traits_searched
        
        if f_name is not None:
            if 'TraitsFolders' not in self.data.keys():
                raise NameError('No TraitsFolders Data has been imported')
            else:
                df_tf = self.data['TraitsFolders'][['folderId','name']].add_prefix('tf_')
            df_trait = df_trait.merge(df_tf,how='left',left_on='folderId',right_on='tf_folderId')
            del df_trait['tf_folderId']
            df_traits_searched=df_trait[df_trait['tf_name'].contains(f_name)]
            return df_traits_searched
        
    def traitChange(self,columnToChange,oldValue=None,newValue=None,condition=None):
        """
        This method enable you to change traits rules based on condition you have set in the parameters. Returns a dataframe of the changed rules.
        parameters : 
            columnToChange : REQUIRED : Column to look for applying the change
            oldValue : REQUIRED : Value to be replaced (ReGex)
            newValue : REQUIRED : Value that will be set (ReGex)
            condition : OPTIONAL : dictionary such as {<col>:<condition to look for>}, if you want to preselect specific trait. The condition is match exactly.
            only one condition supported at the moment.
            
        tip : in order to add something, use regular expression match groups as the following
            traitChangeRule('(matchElement.+)','\\1 AND (myNewConidtion == myValue)'
            from the trait rule : "(matchElement == "something")"
            it will become : "(matchElement == "something") AND (myNewConidtion == myValue)"
        """
        if 'Traits' not in self.data.keys():
            raise NameError('No Trait Data has been imported')
        df_trait = self.data['Traits']
        df_trait.fillna('',inplace=True)
        if columnToChange not in list(df_trait.columns):
            raise NameError('No column named : '+str(columnToChange))
        if type(condition)== dict:
            col = list(condition.keys())[0]
            val = condition[col]
            try:
                val = int(val)
            except:
                val = val
            df_trait = df_trait[df_trait[col] == val]
        df_trait[columnToChange] = df_trait[columnToChange].str.replace(oldValue,newValue)
        df_return_traits = df_trait[df_trait[columnToChange].str.contains(newValue)]
        return df_return_traits
    def __checkNumber(data):
        try:
            new_data = int(data)
        except: 
            new_data = data
        return new_data
    
    def segmentsSearch(self,name=None,sid=None,rule=None,ts_id=None,f_id=None,i_1=None,i_30=None,i_lt=None,t_1=None,t_30=None,t_lt=None,ts_name=None,f_name=None):
        """
        This method will return the different traits that match the search criteria.
        parameters : 
            name : will search for the pattern you have used using ReGex in the name
            sid : will try to match EXACTLY your sid
            rule : will search for the pattern you have used using ReGex in the name
            ts_id : will look if the segment contains your trait source id
            ts_name : REQUIRED the Segment Folder data table : will look at the segments that contains your trait name (using REGEX). 
            f_id = will try to match EXACTLY your folder ID
            f_name = REQUIRED Traits data table : will search for the pattern you have used using ReGex in the Folder name
            i_1 : will look for segments that has AT LEAST the number of Instant Unique 1 day
            i_30 : will look for segments that has AT LEAST the number of Instant Unique 30 day
            i_lt : will look for segments that has AT LEAST the number of Instant Unique on lifetime
            t_1 : will look for segment that has AT LEAST the number of Total Unique 1 day
            t_30 : will look for segment that has AT LEAST the number of Total Unique 30 day
            t_lt : will look for segment that has AT LEAST the number of Total Unique lifetime
        """
        if 'Segments' not in self.data.keys():
            raise NameError('No Segments Data has been imported')
        df_segments = self.data['Segments']
        df_segments.fillna('',inplace=True)
        if name is not None:
            df_segments_searched=df_segments[df_segments['name'].str.contains(name,flags=_re.IGNORECASE)]
            return df_segments_searched
        if sid is not None:
            sid = self.__checkNumber(sid)
            df_segments_searched=df_segments[df_segments['sid'] == sid]
            return df_segments_searched
        if f_id is not None:
            f_id = self.__checkNumber(f_id)
            df_segments_searched=df_segments[df_segments['folderId'] == f_id]
            return df_segments_searched
        if rule is not None:
            df_segments_searched=df_segments[df_segments['segmentRule'].str.contains(rule,flags=_re.IGNORECASE)]
            return df_segments_searched
        if ts_id is not None:
            ts_id = self.__checkNumber(ts_id)
            indexes = []
            for index, ids in df_segments['traitDataSourceIds'].iteritems():
                for nested_ids in ids :
                    if ts_id == nested_ids:
                        indexes.append(index)
            bool_values = df_segments.index.isin(indexes)
            df_segments_searched = df_segments[bool_values]
            return df_segments_searched
        if i_1 is not None: 
            i_1 = self.__checkNumber(i_1)
            df_segments_searched = df_segments[df_segments['instantUniques1Day'] >=i_1]
            return df_segments_searched
        if i_30 is not None: 
            i_30 = self.__checkNumber(i_30)
            df_segments_searched = df_segments[df_segments['instantUniques30Day'] >=i_30]
            return df_segments_searched
        if i_lt is not None: 
            i_lt = self.__checkNumber(i_lt)
            df_segments_searched = df_segments[df_segments['instantUniquesLifetime'] >=i_lt]
            return df_segments_searched
        if t_1 is not None: 
            t_1 = self.__checkNumber(t_1)
            df_segments_searched = df_segments[df_segments['totalUniques1Day'] >=t_1]
            return df_segments_searched
        if t_30 is not None: 
            t_30 = self.__checkNumber(t_30)
            df_segments_searched = df_segments[df_segments['totalUniques30Day'] >=t_30]
            return df_segments_searched
        if t_lt is not None: 
            t_lt = self.__checkNumber(t_lt)
            df_segments_searched = df_segments[df_segments['totalUniquesLifetime'] >=t_lt]
            return df_segments_searched
        ###require additional data
        if ts_name is not None:
            if 'Traits' not in self.data.keys():
                raise NameError('No Traits Data has been imported')
            else:
                df_traits = self.traitSearch(name=ts_name)['sid']
                list_traits_id = list(df_traits['sid'].unique())
            indexes = []
            for index, ids in df_segments['traitDataSourceIds'].iteritems():
                for nested_ids in ids :
                    for ids in list_traits_id:
                        if ids == nested_ids:
                            indexes.append(index)
            bool_values = df_segments.index.isin(indexes)
            df_segments_searched = df_segments[bool_values]
            return df_segments_searched
        if f_name is not None:
            if 'SegmentsFolders' not in self.data.keys():
                raise NameError('No SegmentsFolders Data has been imported')
            else:
                df_sf = self.data['SegmentsFolders'][['folderId','name']].add_prefix('sf_')
            df_segments = df_segments.merge(df_sf,how='left',left_on='folderId',right_on='sf_folderId')
            del df_segments['sf_folderId']
            df_segments_searched=df_segments[df_segments['tf_name'].contains(f_name)]
            return df_segments_searched
        
    def createTemplateNew(self,*arguments):
        """
        This method creates a csv file to guide you on creating new traits or segments.
        It also creates/updates 1 instance object (template) that contains these informations. 
        Possible arguments : 
            traits : create the template for new traits
            segments : create the template for new segments
        """
        for arg in arguments:
            if arg == 'traits' or arg =='Traits':
                self.template['new_traits'] = _pd.DataFrame(self._Template['traits']['new'])
                _pd.DataFrame(self._Template['traits']['new']).to_csv('new_traits.csv',index=False,sep='\t')
                print('new_traits.csv has been created in your working folder')
            if arg == 'segments' or arg =='Segments':
                self.template['new_segments'] = _pd.DataFrame(self._Template['segments']['new'])
                _pd.DataFrame(self._Template['segments']['new']).to_csv('new_segments.csv',index=False,sep='\t')
                print('new_segments.csv has been created in your working folder')
    
    def createTemplateUpdate(self,*arguments):
        """
        This method creates a csv file to guide you on updating traits or segments.
        It also creates/updates 1 instance object (template) that contains these informations. 
        Possible arguments : 
            traits : create the template for updating traits
            segments : create the template for updating segments
        """
        for arg in arguments:
            if arg == 'traits' or arg =='Traits':
                self.template['update_traits'] = _pd.DataFrame(self._Template['traits']['update'])
                _pd.DataFrame(self._Template['traits']['update']).to_csv('update_traits.csv',index=False,sep='\t')
                print('update_traits.csv has been created in your working folder')
            if arg == 'segments' or arg =='Segments':
                self.template['update_segments'] = _pd.DataFrame(self._Template['segments']['update'])
                _pd.DataFrame(self._Template['segments']['update']).to_csv('update_segments.csv',index=False,sep='\t')
                print('update_segments.csv has been created in your working folder')