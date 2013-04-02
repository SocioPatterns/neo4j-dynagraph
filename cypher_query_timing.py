#!/usr/bin/env python
#
# Perform a number of Cypher queries and compute median execution times

# Copyright (C) 2013 ISI Foundation
# written by Ciro Cattuto <ciro.cattuto@isi.it>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import sys, time
from neo4jrestclient import client

NEO4J_URL = sys.argv[1]
RUN_NAME = sys.argv[2]

gdb = client.GraphDatabase(NEO4J_URL)

# get the IDs of a few fixed nodes to be used for test queries below

ret = gdb.query(q="""START root=node(0) MATCH root-[:HAS_RUN]->run WHERE run.name = "%s" RETURN run""" % RUN_NAME, returns=client.Node)[0]
RUN_ID = ret[0]._get_id()

ret = gdb.query(q="""START run=node(%d) MATCH run-[:RUN_FRAME]->frame WHERE frame.frame_id = %d RETURN frame""" % (RUN_ID, 8084), returns=client.Node)[0]
FRAME_ID = ret[0]._get_id()

ret = gdb.query(q="""START run=node(%d) MATCH run-[:RUN_TAG]->tag WHERE tag.tag = %d RETURN tag""" % (RUN_ID, 1138), returns=client.Node)[0]
TAG_ID = ret[0]._get_id()

TAG1_ID = TAG_ID
ret = gdb.query(q="""START run=node(%d) MATCH run-[:RUN_TAG]->tag WHERE tag.tag = %d RETURN tag""" % (RUN_ID, 1146), returns=client.Node)[0]
TAG2_ID = ret[0]._get_id()

# =========================================

QUERY1 = """
START root = node(0)
MATCH root-[:HAS_RUN]->run-[:HAS_TIMELINE]->()-[y:NEXT_LEVEL]->()-[m:NEXT_LEVEL]->()-[d:NEXT_LEVEL]->()-[h:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]->frame
WHERE run.name="%s" and y.year=2009 and m.month=7 and d.day=1 and h.hour>=9 and h.hour<13
RETURN frame ORDER BY frame.timestamp
""" % RUN_NAME

QUERY2 = """
START frame = node(%d)
MATCH frame-[:FRAME_TAG]-tag
RETURN tag.name
""" % FRAME_ID

QUERY3 = """
START frame=node(%s)
MATCH frame-[r:FRAME_EDGE]-edge
WHERE r.weight > %d 
RETURN edge.tag1, edge.tag2, r.weight;
""" % (FRAME_ID, 0)

QUERY4 = """
START run = node(%d)
MATCH run-[:RUN_TAG]->tag<-[r:FRAME_TAG]-()
RETURN tag.name, count(r)
""" % RUN_ID

QUERY5 = """
START run = node(%d)
MATCH run-[:RUN_TAG]->tag<-[r:FRAME_TAG]-()
WITH tag.name as name, COUNT(r) as freq
WHERE freq > 1000
RETURN name, freq ORDER BY freq DESC
""" % RUN_ID

QUERY5b = """
START run = node(%d)
MATCH run-[:RUN_TAG]-tag
WITH tag
MATCH ()-[r:FRAME_TAG]-tag
WITH tag.name as name, COUNT(r) as freq
WHERE freq > 1000
RETURN name, freq ORDER BY freq DESC;
""" % RUN_ID

QUERY6 = """
START tag = node(%d)
MATCH ()-[d:NEXT_LEVEL]->()-[:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-()-[:FRAME_TAG]-tag
RETURN DISTINCT(d.day)
""" % TAG_ID

QUERY6b = """
START tag = node(%d)
MATCH frame-[:FRAME_TAG]-tag
RETURN DISTINCT(frame.day)
""" % TAG_ID

QUERY7 = """
START tag1 = node(%d)
MATCH tag1<-[:EDGE_TAG]-()-[:EDGE_TAG]->tag2
RETURN tag2.name ORDER BY tag2.name
""" % TAG_ID

QUERY8 = """
START tag1 = node(%d)
MATCH tag1<-[:EDGE_TAG]-edge-[:EDGE_TAG]->tag2
WITH edge, tag2
MATCH ()-[d:NEXT_LEVEL]->()-[:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-()-[:FRAME_EDGE]-edge
WHERE d.day = 7
RETURN DISTINCT(tag2.name)
""" % TAG_ID

QUERY9 = """
START tag1 = node(%d), tag2 = node(%d)
MATCH tag1<-[:EDGE_TAG]-()-[:EDGE_TAG]->tag
WITH COLLECT(tag) as neighs1, tag2
MATCH tag2<-[:EDGE_TAG]-()-[:EDGE_TAG]->tag
WHERE tag IN neighs1
RETURN tag
""" % (TAG1_ID, TAG2_ID)

QUERY9b = """
START tag1 = node(%d), tag2 = node(%d)
MATCH tag1<-[:EDGE_TAG]-()-[:EDGE_TAG]->tag<-[:EDGE_TAG]-()-[:EDGE_TAG]->tag2
RETURN tag
""" % (TAG1_ID, TAG2_ID)

QUERY10 = """
START run = node(%d)
MATCH run-[:RUN_TAG]-tag-[r:EDGE_TAG]-()
RETURN tag.name, COUNT(r) ORDER BY COUNT(r) DESC
""" % RUN_ID

# get the tag focused network during the day 29, hour 10

QUERY11a = """
START tag = node(%d)
MATCH neigh1<-[:EDGE_TAG]-edge1-[:EDGE_TAG]->tag,
      ()-[d1:NEXT_LEVEL]->()-[h1:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_EDGE]-edge1
WHERE d1.day = 29 and h1.hour = 10
WITH  distinct neigh1, tag
MATCH neigh2<-[:EDGE_TAG]-edge2-[:EDGE_TAG]->tag,
      ()-[d2:NEXT_LEVEL]->()-[h2:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_EDGE]-edge2
WHERE d2.day = 29 and h2.hour = 10
WITH distinct neigh2, neigh1
MATCH neigh1<-[:EDGE_TAG]-edge3-[:EDGE_TAG]->neigh2,
      ()-[d3:NEXT_LEVEL]->()-[h3:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_EDGE]-edge3
WHERE d3.day = 29 and h3.hour = 10
RETURN distinct neigh1.tag, neigh2.tag ORDER BY neigh1.tag, neigh2.tag;
""" % TAG_ID

ret = gdb.query(q="""
START run = node(%d)
MATCH run-[:HAS_TIMELINE]->()-[y:NEXT_LEVEL]->()-[m:NEXT_LEVEL]->()-[d:NEXT_LEVEL]->()-[h:NEXT_LEVEL]->hour
WHERE d.day = 29 and h.hour = 10
RETURN hour
""" % RUN_ID, returns=client.Node)[0]
HOUR_ID = ret[0]._get_id()

QUERY11b = """
START tag = node(%d), hour = node(%d)
MATCH neigh1<-[:EDGE_TAG]-edge1-[:EDGE_TAG]->tag,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_EDGE]->edge1
WITH  distinct hour, neigh1, tag
MATCH neigh2<-[:EDGE_TAG]-edge2-[:EDGE_TAG]->tag,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_EDGE]->edge2
WITH distinct hour, neigh1, neigh2
MATCH neigh1<-[:EDGE_TAG]-edge3-[:EDGE_TAG]->neigh2,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_EDGE]->edge3
RETURN distinct neigh1.tag, neigh2.tag ORDER BY neigh1.tag, neigh2.tag;
""" % (TAG_ID, HOUR_ID)

QUERY11c = """
START tag = node(%d)
MATCH neigh1<-[:EDGE_TAG]-edge1-[:EDGE_TAG]->tag,
      frame1-[:FRAME_EDGE]->edge1
WHERE frame1.day = 29 and frame1.hour = 10
WITH  distinct neigh1, tag
MATCH neigh2<-[:EDGE_TAG]-edge2-[:EDGE_TAG]->tag,
      frame2-[:FRAME_EDGE]->edge2
WHERE frame2.day = 29 and frame2.hour = 10
WITH  distinct neigh1, neigh2
MATCH neigh1<-[:EDGE_TAG]-edge3-[:EDGE_TAG]->neigh2,
      frame3-[:FRAME_EDGE]->edge3
WHERE frame3.day = 29 and frame3.hour = 10
RETURN distinct neigh1.tag, neigh2.tag ORDER BY neigh1.tag, neigh2.tag;

""" % TAG_ID




QLIST = [
    ('QUERY1', QUERY1), ('QUERY2', QUERY2), ('QUERY3', QUERY3), \
    ('QUERY4', QUERY4), ('QUERY5', QUERY5), ('QUERY6', QUERY6), ('QUERY6b', QUERY6b),\
    ('QUERY7', QUERY7), ('QUERY8', QUERY8), ('QUERY9', QUERY9), \
    ('QUERY10', QUERY10),
    ('QUERY11a', QUERY11a), ('QUERY11b', QUERY11b), ('QUERY11c', QUERY11c) ]

# =========================================

def time_query(gdb, query, N=10):
    tlist = []

    for i in range(N):
        t1 = time.time()
        dummy = list(gdb.query(q=query))
#        print "#" + str(i), len(dummy), "rows"
        t2 = time.time()

        tlist.append(t2-t1)
        
#        sys.stdout.write(".")
#        sys.stdout.flush()

    tlist.sort()

    return tuple([int(tlist[int(x)] * 1000) for x in (N/2, N*0.05, N*0.95)])

# =========================================

for (qname, Q) in QLIST:
    (median, quantile5, quantile95) = time_query(gdb, Q)
    print "%s\t%dms\t(%dms - %dms)" % (qname, median, quantile5, quantile95)

