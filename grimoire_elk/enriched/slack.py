# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2023 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging


from .enrich import Enrich, metadata
from ..elastic_analyzer import Analyzer as BaseAnalyzer
from ..elastic_mapping import Mapping as BaseMapping


logger = logging.getLogger(__name__)

SLACK_URL = "https://slack.com/"


class Analyzer(BaseAnalyzer):
    @staticmethod
    def get_elastic_analyzers(es_major):
        settings = """
            {
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "my_stop_analyzer": {
                                "tokenizer": "lowercase",
                                "filter": [
                                    "english_stop",
                                    "ad_hoc_stop"
                                ]
                            }
                        },
                        "filter": {
                            "ad_hoc_stop": {
                                "type": "stop",
                                "stopwords": [
                                    "'ll","'tis","'twas","'ve","10","39","a","a's","able","ableabout","about","above",
                                    "abroad","abst","accordance","according","accordingly","across","act","actually","ad",
                                    "added","adj","adopted","ae","af","affected","affecting","affects","after","afterwards",
                                    "ag","again","against","ago","ah","ahead","ai","ain't","aint","al","all","allow",
                                    "allows","almost","alone","along","alongside","already","also","although","always","am",
                                    "amid","amidst","among","amongst","amoungst","amount","an","and","announce","another",
                                    "any","anybody","anyhow","anymore","anyone","anything","anyway","anyways","anywhere",
                                    "ao","apart","apparently","appear","appreciate","appropriate","approximately","aq","ar",
                                    "are","area","areas","aren","aren't","arent","arise","around","arpa","as","aside","ask",
                                    "asked","asking","asks","associated","at","au","auth","available","aw","away","awfully",
                                    "az","b","ba","back","backed","backing","backs","backward","backwards","bb","bd","be",
                                    "became","because","become","becomes","becoming","been","before","beforehand","began",
                                    "begin","beginning","beginnings","begins","behind","being","beings","believe","below",
                                    "beside","besides","best","better","between","beyond","bf","bg","bh","bi","big","bill",
                                    "billion","biol","bj","bm","bn","bo","both","bottom","br","brief","briefly","bs","bt",
                                    "but","buy","bv","bw","by","bz","c","c'mon","c's","ca","call","came","can","can't",
                                    "cannot","cant","caption","case","cases","cause","causes","cc","cd","certain",
                                    "certainly","cf","cg","ch","changes","ci","ck","cl","clear","clearly","click","cm",
                                    "cmon","cn","co","co.","com","come","comes","computer","con","concerning",
                                    "consequently","consider","considering","contain","containing","contains","copy",
                                    "corresponding","could","could've","couldn","couldn't","couldnt","course","cr","cry",
                                    "cs","cu","currently","cv","cx","cy","cz","d","dare","daren't","darent","date","de",
                                    "dear","definitely","describe","described","despite","detail","did","didn","didn't",
                                    "didnt","differ","different","differently","directly","dj","dk","dm","do","does",
                                    "doesn","doesn't","doesnt","doing","don","don't","done","dont","doubtful","down",
                                    "downed","downing","downs","downwards","due","during","dz","e","each","early","ec","ed",
                                    "edu","ee","effect","eg","eh","eight","eighty","either","eleven","else","elsewhere",
                                    "empty","end","ended","ending","ends","enough","entirely","er","es","especially","et",
                                    "et-al","etc","even","evenly","ever","evermore","every","everybody","everyone",
                                    "everything","everywhere","ex","exactly","example","except","f","face","faces","fact",
                                    "facts","fairly","far","farther","felt","few","fewer","ff","fi","fifteen","fifth",
                                    "fifty","fify","fill","find","finds","fire","first","five","fix","fj","fk","fm","fo",
                                    "followed","following","follows","for","forever","former","formerly","forth","forty",
                                    "forward","found","four","fr","free","from","front","full","fully","further",
                                    "furthered","furthering","furthermore","furthers","fx","g","ga","gave","gb","gd","ge",
                                    "general","generally","get","gets","getting","gf","gg","gh","gi","give","given","gives",
                                    "giving","gl","gm","gmt","gn","go","goes","going","gone","good","goods","got","gotten",
                                    "gov","gp","gq","gr","great","greater","greatest","greetings","group","grouped",
                                    "grouping","groups","gs","gt","gu","gw","gy","h","had","hadn't","hadnt","half",
                                    "happens","hardly","has","hasn","hasn't","hasnt","have","haven","haven't","havent",
                                    "having","he","he'd","he'll","he's","hed","hell","hello","help","hence","her","here",
                                    "here's","hereafter","hereby","herein","heres","hereupon","hers","herself",
                                    "herse","hes","hi","hid","high","higher","highest","him","himself","himse",
                                    "his","hither","hk","hm","hn","home","homepage","hopefully","how","how'd","how'll",
                                    "how's","howbeit","however","hr","ht","htm","html","http","hu","hundred","i","i'd",
                                    "i'll","i'm","i've","i.e.","id","ie","if","ignored","ii","il","ill","im","immediate",
                                    "immediately","importance","important","in","inasmuch","inc","inc.","indeed","index",
                                    "indicate","indicated","indicates","information","inner","inside","insofar","instead",
                                    "int","interest","interested","interesting","interests","into","invention","inward",
                                    "io","iq","ir","is","isn","isn't","isnt","it","it'd","it'll","it's","itd","itll","its",
                                    "itself","itse","ive","j","je","jm","jo","join","jp","just","k","ke","keep",
                                    "keeps","kept","keys","kg","kh","ki","kind","km","kn","knew","know","known","knows",
                                    "kp","kr","kw","ky","kz","l","la","large","largely","last","lately","later","latest",
                                    "latter","latterly","lb","lc","least","length","less","lest","let","let's","lets","li",
                                    "like","liked","likely","likewise","line","little","lk","ll","long","longer","longest",
                                    "look","looking","looks","low","lower","lr","ls","lt","ltd","lu","lv","ly","m","ma",
                                    "made","mainly","make","makes","making","man","many","may","maybe","mayn't","maynt",
                                    "mc","md","me","mean","means","meantime","meanwhile","member","members","men","merely",
                                    "mg","mh","microsoft","might","might've","mightn't","mightnt","mil","mill","million",
                                    "mine","minus","miss","mk","ml","mm","mn","mo","more","moreover","most","mostly","move",
                                    "mp","mq","mr","mrs","ms","msie","mt","mu","much","mug","must","must've","mustn't",
                                    "mustnt","mv","mw","mx","my","myself","myse","mz","n","na","name","namely","nay",
                                    "nc","nd","ne","near","nearly","necessarily","necessary","need","needed","needing",
                                    "needn't","neednt","needs","neither","net","netscape","never","neverf","neverless",
                                    "nevertheless","new","newer","newest","next","nf","ng","ni","nine","ninety","nl","no",
                                    "no-one","nobody","non","none","nonetheless","noone","nor","normally","nos","not",
                                    "noted","nothing","notwithstanding","novel","now","nowhere","np","nr","nu","null",
                                    "number","numbers","nz","o","obtain","obtained","obviously","of","off","often","oh",
                                    "ok","okay","old","older","oldest","om","omitted","on","once","one","one's","ones",
                                    "only","onto","open","opened","opening","opens","opposite","or","ord","order","ordered",
                                    "ordering","orders","org","other","others","otherwise","ought","oughtn't","oughtnt",
                                    "our","ours","ourselves","out","outside","over","overall","owing","own","p","pa","page",
                                    "pages","part","parted","particular","particularly","parting","parts","past","pe","per",
                                    "perhaps","pf","pg","ph","pk","pl","place","placed","places","please","plus","pm",
                                    "pmid","pn","point","pointed","pointing","points","poorly","possible","possibly",
                                    "potentially","pp","pr","predominantly","present","presented","presenting","presents",
                                    "presumably","previously","primarily","probably","problem","problems","promptly",
                                    "proud","provided","provides","pt","put","puts","pw","py","q","qa","que","quickly",
                                    "quite","qv","r","ran","rather","rd","re","readily","really","reasonably","recent",
                                    "recently","ref","refs","regarding","regardless","regards","related","relatively",
                                    "research","reserved","respectively","resulted","resulting","results","right","ring",
                                    "ro","room","rooms","round","ru","run","rw","s","sa","said","same","saw","say","saying",
                                    "says","sb","sc","sd","se","sec","second","secondly","seconds","section","see","seeing",
                                    "seem","seemed","seeming","seems","seen","sees","self","selves","sensible","sent",
                                    "serious","seriously","seven","seventy","several","sg","sh","shall","shan't","shant",
                                    "she","she'd","she'll","she's","shed","shell","shes","should","should've","shouldn",
                                    "shouldn't","shouldnt","show","showed","showing","shown","showns","shows","si","side",
                                    "sides","significant","significantly","similar","similarly","since","sincere","site",
                                    "six","sixty","sj","sk","sl","slightly","sm","small","smaller","smallest","sn","so",
                                    "some","somebody","someday","somehow","someone","somethan","something","sometime",
                                    "sometimes","somewhat","somewhere","soon","sorry","specifically","specified","specify",
                                    "specifying","sr","st","state","states","still","stop","strongly","su","sub",
                                    "substantially","successfully","such","sufficiently","suggest","sup","sure","sv","sy",
                                    "system","sz","t","t's","take","taken","taking","tc","td","tell","ten","tends","test",
                                    "text","tf","tg","th","than","thank","thanks","thanx","that","that'll","that's",
                                    "that've","thatll","thats","thatve","the","their","theirs","them","themselves","then",
                                    "thence","there","there'd","there'll","there're","there's","there've","thereafter",
                                    "thereby","thered","therefore","therein","therell","thereof","therere","theres",
                                    "thereto","thereupon","thereve","these","they","they'd","they'll","they're","they've",
                                    "theyd","theyll","theyre","theyve","thick","thin","thing","things","think","thinks",
                                    "third","thirty","this","thorough","thoroughly","those","thou","though","thoughh",
                                    "thought","thoughts","thousand","three","throug","through","throughout","thru","thus",
                                    "til","till","tip","tis","tj","tk","tm","tn","to","today","together","too","took","top",
                                    "toward","towards","tp","tr","tried","tries","trillion","truly","try","trying","ts",
                                    "tt","turn","turned","turning","turns","tv","tw","twas","twelve","twenty","twice","two",
                                    "tz","u","ua","ug","uk","um","un","under","underneath","undoing","unfortunately",
                                    "unless","unlike","unlikely","until","unto","up","upon","ups","upwards","us","use",
                                    "used","useful","usefully","usefulness","uses","using","usually","uucp","uy","uz","v",
                                    "va","value","various","vc","ve","versus","very","vg","vi","via","viz","vn","vol",
                                    "vols","vs","vu","w","want","wanted","wanting","wants","was","wasn","wasn't","wasnt",
                                    "way","ways","we","we'd","we'll","we're","we've","web","webpage","website","wed",
                                    "welcome","well","wells","went","were","weren","weren't","werent","weve","wf","what",
                                    "what'd","what'll","what's","what've","whatever","whatll","whats","whatve","when",
                                    "when'd","when'll","when's","whence","whenever","where","where'd","where'll","where's",
                                    "whereafter","whereas","whereby","wherein","wheres","whereupon","wherever","whether",
                                    "which","whichever","while","whilst","whim","whither","who","who'd","who'll","who's",
                                    "whod","whoever","whole","wholl","whom","whomever","whos","whose","why","why'd",
                                    "why'll","why's","widely","width","will","willing","wish","with","within","without",
                                    "won","won't","wonder","wont","words","work","worked","working","works","world","would",
                                    "would've","wouldn","wouldn't","wouldnt","ws","www","x","y","ye","year","years","yes",
                                    "yet","you","you'd","you'll","you're","you've","youd","youll","young","younger",
                                    "youngest","your","youre","yours","yourself","yourselves","youve","yt","yu","z","za",
                                    "zero","zm","zr"]
                            },
                            "english_stop": {
                                "type":       "stop",
                                "stopwords":  "_english_"
                            }
                        }
                    }
                }
            } """
        return {"items": settings}


class Mapping(BaseMapping):

    @staticmethod
    def get_elastic_mappings(es_major):
        """Get Elasticsearch mapping.

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the mapping
        """

        mapping = """
        {
            "properties": {
                "text_analyzed": {
                  "type": "text",
                  "fielddata": true,
                  "analyzer": "my_stop_analyzer"
                }
           }
        } """

        return {"items": mapping}


class SlackEnrich(Enrich):

    analyzer = Analyzer
    mapping = Mapping

    def get_field_author(self):
        return "user_data"

    def get_sh_identity(self, item, identity_field=None):
        identity = {
            'username': None,
            'name': None,
            'email': None
        }

        from_ = item
        if isinstance(item, dict) and 'data' in item:
            if self.get_field_author() not in item['data']:
                # Message from bot. For the rare cases where both user
                # and bot_id are not present, an empty identity is returned
                identity['username'] = item['data'].get('bot_id', None)
                return identity
            from_ = item['data'][self.get_field_author()]

        if not from_:
            return identity
        identity['username'] = from_['name']
        identity['name'] = from_['name']
        if 'real_name' in from_:
            identity['name'] = from_['real_name']
        if 'profile' in from_:
            if 'email' in from_['profile']:
                identity['email'] = from_['profile']['email']
        return identity

    def get_identities(self, item):
        """ Return the identities from an item """

        identity = self.get_sh_identity(item)
        yield identity

    def get_project_repository(self, eitem):
        repo = eitem['origin']
        repo = repo.replace(SLACK_URL, "")  # only the channel id is included for the mapping
        return repo

    @metadata
    def get_rich_item(self, item):
        eitem = {}

        self.copy_raw_fields(self.RAW_FIELDS_COPY, item, eitem)

        # The real data
        message = item['data']

        eitem["reply_count"] = 0  # be sure it is always included

        # data fields to copy
        copy_fields = ["text", "type", "reply_count", "subscribed", "subtype",
                       "unread_count", "user"]
        for f in copy_fields:
            if f in message:
                eitem[f] = message[f]
            else:
                eitem[f] = None

        eitem['text_analyzed'] = eitem['text']

        eitem['number_attachs'] = 0
        if 'attachments' in message and message['attachments']:
            eitem['number_attachs'] = len(message['attachments'])

        eitem['reaction_count'] = 0
        if 'reactions' in message:
            eitem['reaction_count'] = len(message['reactions'])
            eitem['reactions'] = []
            for rdata in message['reactions']:
                # {
                #         "count": 2,
                #         "users": [
                #            "U38J51N7J",
                #            "U3Q0VLHU3"
                #         ],
                #         "name": "+1"
                # }
                for i in range(0, rdata['count']):
                    eitem['reactions'].append(rdata["name"])

        if 'files' in message:
            eitem['number_files'] = len(message['files'])
            message_file_size = 0
            for file in message['files']:
                message_file_size += file.get('size', 0)
            eitem['message_file_size'] = message_file_size

        if 'user_data' in message and message['user_data']:
            eitem['team_id'] = message['user_data']['team_id']
            if 'tz_offset' in message['user_data']:
                eitem['tz'] = message['user_data']['tz_offset']
                # tz must be in -12h to 12h interval, so seconds -> hours
                eitem['tz'] = round(int(eitem['tz']) / (60 * 60))
            if 'is_admin' in message['user_data']:
                eitem['is_admin'] = message['user_data']['is_admin']
            if 'is_owner' in message['user_data']:
                eitem['is_owner'] = message['user_data']['is_owner']
            if 'is_primary_owner' in message['user_data']:
                eitem['is_primary_owner'] = message['user_data']['is_primary_owner']
            if 'profile' in message['user_data']:
                if 'title' in message['user_data']['profile']:
                    eitem['profile_title'] = message['user_data']['profile']['title']
                eitem['avatar'] = message['user_data']['profile']['image_32']

        # Channel info
        channel = message['channel_info']
        eitem['channel_name'] = channel['name']
        eitem['channel_id'] = channel['id']
        eitem['channel_created'] = channel['created']
        # Due to a Slack API change, the list of members is returned paginated, thus the new attribute `num_members`
        # has been added to the Slack Perceval backend. In order to avoid breaking changes, the former
        # variable `members` is kept and used only if `num_members` is not present in the input item.
        eitem['channel_member_count'] = channel['num_members'] if 'num_members' in channel else len(channel['members'])
        if 'topic' in channel:
            eitem['channel_topic'] = channel['topic']
        if 'purpose' in channel:
            eitem['channel_purpose'] = channel['purpose']
        channel_bool_fields = ['is_archived', 'is_general', 'is_starred']
        for field in channel_bool_fields:
            eitem['channel_' + field] = 0
            if field in channel and channel[field]:
                eitem['channel_' + field] = 1

        eitem = self.__convert_booleans(eitem)

        if self.sortinghat:
            eitem.update(self.get_item_sh(item))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem.update(self.get_grimoire_fields(item["metadata__updated_on"], "message"))

        self.add_repository_labels(eitem)
        self.add_metadata_filter_raw(eitem)
        return eitem

    def __convert_booleans(self, eitem):
        """ Convert True/False to 1/0 for better kibana processing """

        for field in eitem.keys():
            if isinstance(eitem[field], bool):
                if eitem[field]:
                    eitem[field] = 1
                else:
                    eitem[field] = 0
        return eitem  # not needed becasue we are modifying directly the dict
