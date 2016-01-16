# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 14:43:35 2016

@author: arthu_000
"""
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
from ggplot import *
import pandas


lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = ["version", "changeset", "timestamp", "user", "uid"]

tags_counter = {'node': 0, 'way': 0}

street_types = [u'gatvė', u'alėja', u'kelias', u'prospektas', u'aikštė', \
    u'skersgatvis', u'plentas', u'akligatvis', u'takas', u'aplinkkelis']
    
street_types_mapping = { 
    'g.': u'gatvė',
    'g': u'gatvė',
    'al.': u'alėja',
    'al': u'alėja',
    'kel.': u'kelias',
    'kel': u'kelias',
    'pr.': u'prospektas',
    'pr': u'prospektas',
    'a.': u'aikštė',
    'a': u'aikštė',
    'skg.': u'skersgatvis',
    'skg': u'skersgatvis',
    'pl.': u'plentas',
    'pl': u'plentas',
    'aklg.': u'akligatvis',
    'aklg': u'akligatvis',
    'tak.': u'takas',
    'tak': u'takas'           
    }


def shape_element(element):
    node = {}
    
    if element.tag == "node" or element.tag == "way":
        # testing only
        tags_counter[element.tag] += 1
            
        # set node type in dictionary
        node['type'] = element.tag
                 
                 
        ### Process data from tag itself
        # first initialise necessary elements
        created_dict = {}    
        latitude = None
        longitude = None        
        # then loop through all atrributes of the tag and process their values
        for attribute in element.attrib.keys():
            # if current attribute is in CREATED then add it to nested 'created' dict
            if attribute in CREATED:
                created_dict[attribute] = element.get(attribute)
            # if attributes are lat or lon extract them for processing in subsequent step
            elif attribute == 'lat':
                latitude = float(element.get(attribute))            
            elif attribute == 'lon':
                longitude = float(element.get(attribute))
            # all other attributes: add the value to its corresponding element
            else:
                node[attribute] = element.get(attribute)        
        
        # if information for CREATED dict was obtained, add to nested dictionary
        if created_dict:
            node['created'] = created_dict

        # if positional information was obtained, add to dictionary
        if latitude and longitude:
            node['pos'] = [latitude, longitude]
            
        
        ### Process data from child tags
        # Loop through all <tag> 
        address_dict = {}    
        for elem_tag in element.iter('tag'):
            #print elem_tag.get('k')
            # Check there no problematic characters
            if re.search(problemchars, elem_tag.get('k')) == None:
                # check if key starts with "addr:", if so process to create address_dict
                elem_tag_split = elem_tag.get('k').partition(':')
                if elem_tag_split[0] == 'addr':
                    # check if there is another ':' in the key, if so ignore                    
                    if ':' not in elem_tag_split[2]:
                        address_dict[elem_tag_split[2]] = elem_tag.get('v')
                # if key does not start with 'addr', process normally by adding to node dict
                else:
                    node[elem_tag.get('k')] = elem_tag.get('v')
            else:
                print 'Problemchars: ', elem_tag.get('k')
             
        # if address information obtained, add to nested dictionary
        if address_dict:
            node['address'] = address_dict
        
    
        # Loop through all <nd>, which is relevant for <way> tags 
        node_refs = []    
        for elem_tag in element.iter('nd'):
            # populate the list of node references
            node_refs.append(elem_tag.get('ref'))
        
        # if nodes obtained, add them to node dictionary as list
        if node_refs:
            node['node_refs'] = node_refs
        
        return node
    
    else:
        return None



def process_map(file_in, pretty_print = False, return_data = False):
    filename_json = "{0}.json".format(file_in)
    
    if return_data:
        data = []
    else: 
        data = None
    
    # open json file output object
    with codecs.open(filename_json, 'w', encoding='utf-8') as file_output:

        # start ET iteration
        for _, element in ET.iterparse(file_in):
            element_dict = shape_element(element)
            if element_dict:
                if return_data:
                    data.append(element_dict)
                    
                if pretty_print:
                    file_output.write(json.dumps(element_dict, encoding='utf8', ensure_ascii=False, indent=2)+"\n")
                else:
                    file_output.write(json.dumps(element_dict, encoding='utf8', ensure_ascii=False) + "\n")
       
    
        print tags_counter
                        
        return data


def fix_streetname(streetname):
                    
    ### Fix abbreviations for street names
    # split the name at rightmost space into a split_string
    streetname_split = streetname.rpartition(' ')
    street_type_abbr = streetname_split[2]
                
    try:
        # Find the full street_type in the mapping dictionary
        street_type_full = street_types_mapping[street_type_abbr]
        
        # Construct mapped streetname
        fixed_streetname = streetname_split[0] + u' ' + street_type_full
        
    except KeyError:
        # In case there is no given street_type in the mapping dictionary
        # (e.g. if someone did put in the full 'gatvė' instead of 'g.' in OSM),
        # then just continue using the initial streetname 
        fixed_streetname = streetname       
        
       
    return fixed_streetname



def clean_streetnames_mongodb(coll):
    
    streets_abbrev = ['al.', 'al',
                      'g.', 'g',
                      'kel.', 'kel',
                      'pr.', 'pr',
                      'a.', 'a',
                      'skg.', 'skg',
                      'pl.', 'pl',
                      'aklg.', 'aklg',
                      'tak.', 'tak']
    
    counter = 0
    
    # iterate through each document in the collection.
    # actually no: create a cursor object that contains only what you want
    # first cursor: only documents that have 'address.street'
    mongo_cursor = coll.find({'address.street': {'$exists': 1}})
    
    # loop through cursor object
    for mongo_doc in mongo_cursor: 
        # testing only
        #if counter == 12:
        #    break
        #counter += 1
        
        #testing only
        #print mongo_doc['address']['street']
        #print type(mongo_doc['address']['street'])        
        #print repr(mongo_doc['address']['street'])
        
        # write fixed streetname instead of old 'address.street'
        fixed_streetname = fix_streetname(mongo_doc['address']['street'])   
        mongo_doc['address']['street'] = fixed_streetname
        coll.save(mongo_doc)
        
        # testing only        
        #print '> ', mongo_doc['address']['street']
    
    
    # second cursor: only documents that have 'name'
    mongo_cursor = coll.find({'name': {'$exists': 1}})
    
    # loop through cursor object
    for mongo_doc in mongo_cursor:     
        # check if "name" actually is a street name
        # use list of street type abbreviations and street_types as reference
        name_split = mongo_doc['name'].rpartition(' ')
        name_ending = name_split[2]
        if name_ending in streets_abbrev or name_ending in street_types:
            # testing only
            #if counter == 12:
            #    break
            #counter += 1
            
            #testing only
            #print mongo_doc['name']
            #print type(mongo_doc['address']['street'])        
            #print repr(mongo_doc['address']['street'])
            
            # write fixed streetname instead of old 'address.street'
            fixed_streetname = fix_streetname(mongo_doc['name'])   
            mongo_doc['name'] = fixed_streetname
            coll.save(mongo_doc)
            
            # testing only        
            #print '> ', mongo_doc['name']

    pass


def clean_first_and_last_names_in_streets(coll):

    # create different cursors 
    mongo_cursor_1 = coll.find({'address.street': {'$exists': 1}})
    mongo_cursor_2 = coll.find({'name': {'$exists': 1}})
    mongo_cursor_3 = coll.find({'$or': [{'address.street': {'$exists': 1}}, {'name': {'$exists': 1}}]})

    # Create a set of unique values for 'name' and 'address.street' in the DB
    # this list will later serve as reference for look-up of abbreviated street names
    reference_set = set()
    for mongo_doc in mongo_cursor_3:
        try:
            reference_set.add(mongo_doc['address']['street'])
        except KeyError:
            # Check if 'name' really is a street name by checking against set of street types
            name_ending = mongo_doc['name'].rpartition(' ')       
            if name_ending in street_types: 
                reference_set.add(mongo_doc['name'])
                
        try:
            # Check if 'name' really is a street name by checking against set of street types
            name_ending = mongo_doc['name'].rpartition(' ')       
            if name_ending in street_types: 
                reference_set.add(mongo_doc['name'])
        except KeyError:
            reference_set.add(mongo_doc['address']['street'])
        
        
    ### Process 'address.street'
    # Loop through first cursor to process each street name in address.street
    for mongo_doc in mongo_cursor_1:
        streetname_old = mongo_doc['address']['street']
        
        # split streetname at first space from the left
        streetname_split = streetname_old.partition(' ')
    
        # Check if the current streetname starts with an abbreviation
        if streetname_split[0].endswith('.'):
    
            # split streetname again to prepare next step
            streetname_split_2 = streetname_split[2].partition(' ') 
            # Check if the streetname has a second abbreviation in the beginning 
            # (double first name)
            if streetname_split_2[0].endswith('.'):               
                # Loop through reference set to see if a street exists
                # that bears the same remainder of the streetname
                for ref_street in reference_set:
    
                    # If we find such an occasion, let's transform
                    # Last check is to make sure first name is really the same
                    # (two streets can have identical last name but different first names)
                    # so it compares the initial with the first letter of the ref_street
                    if streetname_split_2[2] in ref_street \
                        and streetname_old != ref_street \
                        and streetname_split[0][0] == ref_street[0]:
                            
                            #print mongo_doc['address']['street']
                            mongo_doc['address']['street'] = ref_street  
                            #print '> ', mongo_doc['address']['street']
                            coll.save(mongo_doc)
                            break
                                         

            # If there is no second abbreviation             
            else:
                # Loop through reference set to see if a street exists
                # that bears the same remainder of the streetname
                for ref_street in reference_set:    
                    # If we find such an occasion, let's transform
                    # Last check is to make sure first name is really the same
                    # (two streets can have identical last name but different first names)
                    # so it compares the initial with the first letter of the ref_street
                    if streetname_split[2] in ref_street \
                        and streetname_old != ref_street \
                        and streetname_split[0][0] == ref_street[0]:
                            
                            #print mongo_doc['address']['street']
                            mongo_doc['address']['street'] = ref_street  
                            #print '> ', mongo_doc['address']['street']
                            coll.save(mongo_doc)         
                            break                                
                            
    #print '**************************************'    
    
    
    ### Process 'name'
    # Loop through second cursor to process each 'name'
    for mongo_doc in mongo_cursor_2:
        streetname_old = mongo_doc['name']
                
        # Check if 'name' really is a street name by checking against set of street types
        streetname_old_split = streetname_old.rpartition(' ')   
        streetname_old_ending = streetname_old_split[2]
        if streetname_old_ending in street_types:       
                
            # split streetname at first space from the left
            streetname_split = streetname_old.partition(' ')
        
            # Check if the current streetname starts with an abbreviation
            if streetname_split[0].endswith('.'):
        
                # split streetname again to prepare next step
                streetname_split_2 = streetname_split[2].partition(' ') 
                # Check if the streetname has a second abbreviation in the beginning 
                # (double first name)
                if streetname_split_2[0].endswith('.'):               
                    # Loop through reference set to see if a street exists
                    # that bears the same remainder of the streetname
                    for ref_street in reference_set:
                        # If we find such an occasion, let's transform
                        # Last check is to make sure first name is really the same
                        # (two streets can have identical last name but different first names)
                        # so it compares the initial with the first letter of the ref_street
                        if streetname_split_2[2] in ref_street \
                            and streetname_old != ref_street \
                            and streetname_split[0][0] == ref_street[0]:
                                
                                #print mongo_doc['name']
                                mongo_doc['name'] = ref_street  
                                #print '> ', mongo_doc['name']
                                coll.save(mongo_doc)
                                break
                                                      
    
                # If there is no second abbreviation             
                else:
                    # Loop through reference set to see if a street exists
                    # that bears the same remainder of the streetname
                    for ref_street in reference_set:        
                        # If we find such an occasion, let's transform
                        # Last check is to make sure first name is really the same
                        # (two streets can have identical last name but different first names)
                        # so it compares the initial with the first letter of the ref_street
                        if streetname_split[2] in ref_street \
                            and streetname_old != ref_street \
                            and streetname_split[0][0] == ref_street[0]:
                                
                                #print mongo_doc['name']
                                mongo_doc['name'] = ref_street  
                                #print '> ', mongo_doc['name'] 
                                coll.save(mongo_doc) 
                                break                                       

    pass


def write_file_consolidated_streetnames(coll):

    unique_streets = set()
    
    # process 'address.street' values
    mongo_cursor = coll.distinct('address.street')
    for mongo_doc in mongo_cursor:
        unique_streets.add(mongo_doc)
        
    # process 'name' values that correspond to LT street naming scheme
    mongo_cursor = coll.distinct('name')
    for mongo_doc in mongo_cursor:
        name_split = mongo_doc.rpartition(' ')
        name_ending = name_split[2]
        if name_ending in street_types:
            unique_streets.add(mongo_doc)


    print len(unique_streets)
    unique_streets = sorted(unique_streets)

    with codecs.open(r'vilnius_streets_mongodb', 'w', encoding='utf-8') as file_output:
        file_output.writelines('%s\n' %street for street in unique_streets)
        
    
    return unique_streets


def detect_streetnames_without_ending(streetnames):
    
    # Loop through all streetnames in the set
    for streetname in streetnames:
        # If streetname does not end with one of the accepted street types
        streetname_split = streetname.rpartition(' ')
        streetname_ending = streetname_split[2]
        if streetname_ending not in street_types:
            # print to screen
            print streetname
    
    pass



### ANALYSIS FUNCTIONS START HERE

def plot_user_contributions(coll):

    # get the count of contributions from unique users to the map 
    pipeline = [{'$group': {'_id': '$created.user',
                            'count': {'$sum': 1}}},
                {'$sort': {'count': 1}}]    
                
    result = coll.aggregate(pipeline)

    # Create DataFrame from pymongo cursor object    
    plot_data_df = pandas.DataFrame(list(result))
    # calculate share of contributions
    plot_data_df['share'] = plot_data_df['count'] / plot_data_df['count'].sum()
    # sort in descending order to facilitate plotting of top contributors
    plot_data_df = pandas.DataFrame.sort(plot_data_df, 'share', ascending=False)    
        
    # plot relative frequency of contribution per user
    gg = ggplot(plot_data_df[:20], aes(x='_id', y='share')) \
        + geom_bar(stat='identity') \
        + ggtitle('Top 20 contributors to Vilnius OSM') \
        + xlab('Contributors') \
        + ylab('Share in total contributions') \
        + theme(axis_text_x  = element_text(angle = 45, hjust = 1)) \
        + scale_y_continuous(labels='percent')

    print gg
    
    pass
        
        
def show_streets_w_most_landmarks(coll):
    
    # get the count of documents per street
    pipeline = [{'$group': {'_id': '$address.street',
                            'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {"$limit":11}]    
                
    result = coll.aggregate(pipeline)
    
    for doc in result:
        print doc['_id'], ': ', doc['count']

    
    pass
        
def explore_amenities(coll):    
    # get frequency of amenity types 
    pipeline = [{"$group":{"_id": "$amenity", 
                           "count": {"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":11}]
    
    for doc in coll.aggregate(pipeline):
        print doc['_id'], ': ', doc['count']
        
    print '\nTOP fast food types:'
    
    pipeline = [{'$match': {'amenity': 'fast_food'}},
                {"$group":{"_id": "$name", 
                           "count": {"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":11}] 

    for doc in coll.aggregate(pipeline):
        print doc['_id'], ': ', doc['count']
    
        
    pass
        

def show_top_streets_for_given_amenity(coll):
    
    # get streets that have most amenities
    pipeline = [
        # Filter documents that have 'address.street' and a given amenity        
        {'$match': {'$and': [ {'address.street': {'$ne': None}}, {'amenity': 'restaurant'} ] }},        
        
        # Group by 'address.street' and 'amenity'
        {'$group': {'_id': {'streetname': '$address.street',
                            'amenity_type': '$amenity'},
                    'count': {'$sum': 1}
        }},

        {"$sort":{"count":-1}},
        {"$limit":10}
        ]
        
            
    for doc in coll.aggregate(pipeline):
        print doc['_id']['amenity_type'], ' in ', doc['_id']['streetname'], ': ', doc['count']

    pass        
        
        
def show_top_streets_for_total_amenities(coll):
    pipeline = [
        # Filter only entries that have 'address.street' and 'amenity'
        {'$match': {'$and': [ {'address.street': {'$ne': None}}, {'amenity': {'$ne': None}} ] }},
        
        # Group by 'address.street' and 'amenity'
        {'$group': {'_id': {'streetname': '$address.street',
                            'amenity_type': '$amenity'},
                    'count': {'$sum': 1}
        }},
        
        # Group by streetname only, count total number of amenities
        {'$group': {'_id': {'streetname': '$_id.streetname'},
                    'count': {'$sum': '$count'},
                    'distinct_count': {'$sum': 1}
        }},
        
        #{"$sort":{"_id.streetname":1}},
        {'$sort': {'count': -1}},
        {"$limit":10}
        ]
            
    for doc in coll.aggregate(pipeline):
        #print doc['_id']['amenity_type'], ' in ', doc['_id']['streetname'], ': ', doc['count']
        print doc['_id']['streetname'], ': ', doc['count'], '(' , doc['distinct_count'], 'different types)'
                  
    pass

        
def show_top_amenity_on_top_streets(coll):
    
    pipeline = [
        # Filter only entries that have 'address.street' and 'amenity'
        {'$match': {'$and': [ {'address.street': {'$ne': None}}, {'amenity': {'$ne': None}} ] }},
        
        # Group by 'address.street' and 'amenity'
        {'$group': {'_id': {'streetname': '$address.street',
                            'amenity_type': '$amenity'},
                    'count': {'$sum': 1} }},
        
        # this sort is important to be able to prepare '$first' in the next group phase
        {'$sort': {'count': -1}},

        # Group by streetname only, count total number of amenities but count also grand total
        {'$group': {'_id': {'streetname': '$_id.streetname'},
                    'top_amenity': {'$first': '$_id.amenity_type'},
                    'top_amenity_count': {'$first':'$count'},
                    'total_amenity_count': {'$sum': '$count'} }},
        
        # Sort streets by total number of amenities
        {'$sort': {'total_amenity_count': -1}},
        {"$limit":10}
        ]
        
    
    for doc in coll.aggregate(pipeline):
        print doc['_id']['streetname'], ':', doc['total_amenity_count'], \
            'amenities. Top amenity:', doc['top_amenity'], 'x', \
            doc['top_amenity_count'], '.'
    
    pass
        
        
def get_db(db_name, coll_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    coll = db.dataset
    coll = db[coll_name]
    return db, coll
    
    
def run_mongodb_query(coll):
    
    pipeline = [
        # Filter documents that have 'address.street' and a given amenity        
        {'$match': {'amenity': 'fast_food'}},        
        
        # Group by 'address.street' and 'amenity'
        {'$group': {'_id': '$name',
                    'count': {'$sum': 1}}},

        {"$sort":{"count":-1}},
        {"$limit":20}]
        
    
    print len(list(coll.aggregate(pipeline)))    
    
    for doc in coll.aggregate(pipeline):
        #print doc['_id']['amenity_type'], ' in ', doc['_id']['streetname'], ': ', doc['count']
        #print doc['_id']['streetname'], ': ', doc['count']
        print doc['_id'], doc['count']
           
    pass



if __name__ == "__main__":

    ### IMPORTING DATA TO MONGODB
    
    # Transform XML datafile to JSON datafile for import to MongoDB
    # pretty_print = False when using full dataset to reduce file space & processing time
    # return_data = False when using full dataset to avoid memory overrun
    data = process_map(r'vilnius_lithuania.osm', False, False)
    
    # Import to MongoDB done via OS shell using mongoimport

    # Initialise MongoDB objects
    db, coll = get_db('tury', 'vilnius_osm')
       
    
    ### CLEANING    
    
    # Clean up streetnames
    clean_streetnames_mongodb(coll)
    
    # Check for redundant streetnames due to abbreviation of first names, etc.
    clean_first_and_last_names_in_streets(coll)    
    
    # Create file with consolidated unique street names
    consolidated_streetnames = write_file_consolidated_streetnames(coll)
    
    # Check if the file contains any streetnames without the expected 
    # denominator for street type - need to correct those manually
    detect_streetnames_without_ending(consolidated_streetnames)
       
       
    ### ANALYSING
      
    # general function to test queries on MongoDB
    #run_mongodb_query(coll)
      
    # who are top contributing users?
    plot_user_contributions(coll)    
    
    # which streets have most landmarks on them?
    show_streets_w_most_landmarks(coll)
    
    # What amenities (and types of amenities) are most frequent?
    explore_amenities(coll)
    
    # Which streets have the most of a given type of amenities?
    show_top_streets_for_given_amenity(coll)
    
    # Which streets have the most amenities in total?
    show_top_streets_for_total_amenities(coll)
    
    # What is the top amenity on each of the top-amenities streets?
    show_top_amenity_on_top_streets(coll)
    
    
    
