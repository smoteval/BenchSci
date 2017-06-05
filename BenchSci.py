from xml.dom import minidom
import urllib.request
import pandas as pd
import json



# Given a pmcid, return a document object for the parsed xml file
def load_xml(pmcid):
    link = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id=%s" % str(pmcid)
    xml_str = urllib.request.urlopen(link).read()
    xmldoc = minidom.parseString(xml_str)
    return xmldoc


# used for getting content of <p> tags
def getNodeText(node):
    nodelist = node.childNodes
    result = []
    for n in nodelist:
        if n.nodeType == n.TEXT_NODE:
            result.append(n.data)
    return ''.join(result)


# input: the body node of a paper document
# output: the contents of all <p> tags that are children of <sec> tags (list of all words)
# Note: This will not get the extra stuff like things under caption of figures
def getBodyText(node):
    result = []

    # get a list of sections
    sections = node.getElementsByTagName('sec')

    # for each section get the text of it's immediate children (only if its in <p> tags)
    for s in sections:
        s_list = s.childNodes
        for p in s_list:
            try:
                if p.tagName == 'p':
                    result.append(getNodeText(p))
            except:
                pass
    return ''.join(result).split(' ')


# Input: List of pmcids
# output: A dataframe containing the required info for each pmcid in the list
def df_create(pmcids):
    frames = []
    for id in pmcids:
        try:
            parsedDoc = load_xml(id)
            body = parsedDoc.getElementsByTagName('body')[0]
            bodyTextList = getBodyText(body)
            bodyTextSet = set(bodyTextList)
            figs = body.getElementsByTagName("fig")
        except:
            figs = []

        dict_figs = []
        for fig in figs:
            fig_id = fig.getAttribute('id')
            fig_captions = fig.getElementsByTagName('caption')
            if len(fig_captions) != 0:
                fig_cap = fig_captions[0]
                cap_p = fig_cap.getElementsByTagName('p')
                if len(cap_p) != 0:
                    caption = getNodeText(cap_p[0])
                    caption_set = set(caption.split(' '))
            graphic = fig.getElementsByTagName('graphic')
            if len(graphic) != 0:
                url = graphic[0].getAttribute('xlink:href')

            co_words = list(caption_set & bodyTextSet)
            dict_fig = {'pmcid': id,
                        'fig_id': fig_id,
                        'co_occurance': co_words,
                        'url_ref': url,
                        'co_occurance_count': len(co_words)
                        }
            dict_figs.append(dict_fig)

        frames.append(pd.DataFrame(dict_figs))

    result = pd.concat(frames)
    result = result.set_index(['pmcid', 'fig_id'])
    return result


# The final function that creates the table we want and writes to files
# Side_effects: creates csv and jason files
# output: DataFrame of our data created by df_create()
def final_df():
    # extract the pmcids from file into a list
    print('loading the pmcids')
    pmcids = []
    with open("pmcids.txt") as file:
        for line in file:
            line = line.strip()
            pmcids.append(line)
    pmcids = pmcids[1:]

    # create df of all data and write into csv and json files
    print('A dataframe is being created for all our data. It will take time...')
    df = df_create(pmcids)

    print('A csv file is being created for our data')
    df.to_csv('data.csv')

    print('A json file is being created for our data')
    j_str = df.to_json(orient='records')
    with open('data.json', 'w') as fp:
        json.dump(j_str, fp)

    print('An HTML table is being created')
    df.to_html('BenchSci_PMC.html')
    return df




# input: dataframe of our data
# side-effect: loads the data into a mongodb (nosql) database
def mongo_stuff(df):
    import pymongo
    client = pymongo.MongoClient('some database')
    db = client.test
    collection = db.collection
    collection.insert_many(df.to_dict('data'))
    client.close()
    return


df = final_df()
