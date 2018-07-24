# -*- coding: utf-8 -*-
#
#
# Copyright (C) 2015 Bitergia
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging

from .utils import get_time_diff_days, grimoire_con

from .enrich import Enrich, metadata

logger = logging.getLogger(__name__)


class DiscourseEnrich(Enrich):

    def __init__(self, db_sortinghat=None, db_projects_map=None, json_projects_map=None,
                 db_user='', db_password='', db_host=''):
        super().__init__(db_sortinghat, db_projects_map, json_projects_map,
                         db_user, db_password, db_host)
        self.categories = {}  # Map from category_id to category_name
        self.categories_tree = {}  # Categories with subcategories

    def get_identities(self, item):
        """ Return the identities from an item """
        identities = []

        # All identities are in the post stream
        # The first post is the question. Next replies

        posts = item['data']['post_stream']['posts']

        for post in posts:
            user = self.get_sh_identity(post)
            identities.append(user)
        return identities

    def get_sh_identity(self, post, identity_field=None):
        identity = {}

        if identity_field in post:
            post = post[identity_field]

        identity['username'] = post['username']
        identity['email'] = None
        identity['name'] = post['display_username']
        return identity

    def get_field_author(self):
        return 'author'

    def get_project_repository(self, eitem):
        return str(eitem['category_id'])

    def get_users_data(self, post):
        """ Adapt the data to be used with standard SH enrich API """
        poster = {}
        poster[self.get_field_author()] = post
        return poster

    def get_rich_item_answers(self, item):
        answers_enrich = []
        nanswers = 0

        for answer in item['data']['post_stream']['posts']:
            eanswer = self.get_rich_item(item)  # reuse all fields from item
            eanswer['id'] = answer['id']
            eanswer['url'] = eanswer['origin'] + "/t/" + answer['topic_slug']
            eanswer['url'] += "/" + str(answer['topic_id']) + "/" + str(answer['post_number'])
            eanswer['type'] = 'answer'
            eanswer.update(self.get_grimoire_fields(answer['created_at'], eanswer['type']))
            eanswer.pop('is_discourse_question')
            eanswer['display_username'] = answer['display_username']
            eanswer['username'] = answer['username']
            eanswer['author_id'] = answer['user_id']
            eanswer['author_trust_level'] = answer['trust_level']
            eanswer['author_url'] = eanswer['origin'] + "/users/" + str(eanswer['author_id'])
            eanswer['reads'] = answer['reads']
            eanswer['score'] = answer['score']
            eanswer['reply_count'] = answer['reply_count']
            eanswer['time_from_question'] = None
            post_time = answer['created_at']
            item_time = item['data']['created_at']
            eanswer['first_reply_time'] = get_time_diff_days(item_time, post_time)
            answers_enrich.append(eanswer)

            if self.sortinghat:
                eanswer.update(self.get_item_sh(answer, date_field="created_at"))

            if self.prjs_map:
                eanswer.update(self.get_item_project(eanswer))

            nanswers += 1
            eanswer['first_answer'] = 0
            if nanswers == 1:
                eanswer['first_answer'] = 1

            if 'accepted_answer' not in answer:
                answer['accepted_answer'] = False

            eanswer['is_accepted_answer'] = 1 if answer['accepted_answer'] else 0
            eanswer['answer_status'] = "accepted" if answer['accepted_answer'] else "not_accepted"

        return answers_enrich

    def __collect_categories(self, origin):
        categories = {}
        con = grimoire_con()
        raw_site = con.get(origin + "/site.json")
        for cat in raw_site.json()['categories']:
            categories[cat['id']] = cat['name']
        return categories

    def __collect_categories_tree(self, origin):
        tree = {}
        con = grimoire_con()
        raw = con.get(origin + "/categories.json")
        raw_json = raw.json()
        if "category_list" in raw_json and 'categories' in raw_json["category_list"]:
            categories = raw_json["category_list"]['categories']
        else:
            return tree
        for cat in categories:
            if 'subcategory_ids' in cat:
                tree[cat['id']] = cat['subcategory_ids']
            else:
                tree[cat['id']] = {}
        return tree

    def __related_categories(self, category_id):
        """ Get all related categories to a given one """
        related = []
        for cat in self.categories_tree:
            if category_id in self.categories_tree[cat]:
                related.append(self.categories[cat])
        return related

    def __show_categories_tree(self):
        """ Show the category tree: list of categories and its subcategories """
        for cat in self.categories_tree:
            print("%s (%i)" % (self.categories[cat], cat))
            for subcat in self.categories_tree[cat]:
                print("-> %s (%i)" % (self.categories[subcat], subcat))

    @metadata
    def get_rich_item(self, item):

        # Get the categories name if not already done
        if not self.categories:
            logger.info("Getting the categories data from %s", item['origin'])
            self.categories = self.__collect_categories(item['origin'])
        # Get the categories tree if not already done
        if not self.categories_tree:
            logger.info("Getting the categories tree data from %s", item['origin'])
            self.categories_tree = self.__collect_categories_tree(item['origin'])
            # self.__show_categories_tree()

        eitem = {}

        for f in self.RAW_FIELDS_COPY:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None

        topic = item['data']

        # Fields that are the same in item and eitem
        copy_fields = ["id"]
        for f in copy_fields:
            if f in topic:
                eitem[f] = topic[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {"like_count": "question_like_count",
                      "posts_count": "question_posts_count",
                      "participant_count": "question_participants",
                      "pinned_at": "question_pinned_at",
                      "pinned_globally": "question_pinned_globally",
                      "pinned_until": "question_pinned_until",
                      "pinned": "question_pinned",
                      "fancy_title": "question_title",
                      "views": "question_views",
                      "reply_count": "question_replies"
                      }
        for fn in map_fields:
            if fn in topic:
                eitem[map_fields[fn]] = topic[fn]
            else:
                eitem[map_fields[fn]] = None

        # The first post is the first published, and it is the question
        first_post = topic['post_stream']['posts'][0]

        eitem['question_title'] = eitem['question_title'][:self.KEYWORD_MAX_SIZE]
        eitem['category_id'] = topic['category_id']
        eitem['categories'] = self.__related_categories(topic['category_id'])
        if topic['category_id'] in self.categories:
            eitem['category_name'] = self.categories[topic['category_id']]
            eitem['categories'] += [eitem['category_name']]
        eitem['url'] = eitem['origin'] + "/t/" + first_post['topic_slug']
        eitem['url'] += "/" + str(first_post['topic_id']) + "/" + str(first_post['post_number'])
        eitem['display_username'] = first_post['display_username']
        eitem['username'] = first_post['username']
        eitem['author_id'] = first_post['user_id']
        eitem['author_trust_level'] = first_post['trust_level']
        eitem['author_url'] = eitem['origin'] + "/users/" + str(eitem['author_id'])
        eitem['reads'] = first_post['reads']
        eitem['score'] = first_post['score']
        eitem['reply_count'] = first_post['reply_count']

        eitem['question_has_accepted_answer'] = 0
        eitem['question_accepted_answer_id'] = None
        # First reply time
        eitem['time_from_question'] = None
        firt_post_time = None
        if len(topic['post_stream']['posts']) > 1:
            firt_post_time = first_post['created_at']
            second_post_time = topic['post_stream']['posts'][1]['created_at']
            eitem['first_reply_time'] = get_time_diff_days(firt_post_time, second_post_time)
            answers_id = [p['id'] for p in topic['post_stream']['posts']
                          if 'accepted_answer' in p and p['accepted_answer']]
            eitem['question_accepted_answer_id'] = answers_id[0] if answers_id else None
            eitem['question_has_accepted_answer'] = 1 if eitem['question_accepted_answer_id'] else 0

        if self.sortinghat:
            eitem.update(self.get_item_sh(first_post, date_field="created_at"))

        if self.prjs_map:
            eitem.update(self.get_item_project(eitem))

        eitem['type'] = 'question'
        eitem.update(self.get_grimoire_fields(topic["created_at"], eitem['type']))

        return eitem

    def get_field_unique_id_answer(self):
        return "id"

    def enrich_items(self, ocean_backend):
        items = ocean_backend.fetch()

        nitems = super(DiscourseEnrich, self).enrich_items(ocean_backend)
        logger.info("Total questions enriched: %i", nitems)

        # And now for each item we want also the answers (tops)
        items = ocean_backend.fetch()
        nanswers = 0
        nitems = 0
        rich_item_answers = []

        for item in items:
            nitems += 1
            rich_item_answers += self.get_rich_item_answers(item)

        if rich_item_answers:
            nanswers += self.elastic.bulk_upload(rich_item_answers,
                                                 self.get_field_unique_id_answer())

            if nanswers != len(rich_item_answers):
                missing = len(rich_item_answers) - nanswers
                logger.error("%s/%s missing answers for Discourse",
                             str(missing), str(len(rich_item_answers)))

        return nitems
