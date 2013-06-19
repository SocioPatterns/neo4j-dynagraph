#!/usr/bin/env python
#
# Perform a number of Cypher queries and compute median execution times

# Copyright (C) 2013 ISI Foundation
# written by Ciro Cattuto <ciro.cattuto@isi.it>
# and Andre' Panisson <andre.panisson@isi.it>
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

ret = gdb.query(q="""START run=node(%d) MATCH run-[:RUN_ACTOR]->actor WHERE actor.actor = %d RETURN actor""" % (RUN_ID, 1138), returns=client.Node)[0]
ACTOR_ID = ret[0]._get_id()
ACTOR1_ID = ACTOR_ID

ret = gdb.query(q="""START run=node(%d) MATCH run-[:RUN_ACTOR]->actor WHERE actor.actor = %d RETURN actor""" % (RUN_ID, 1146), returns=client.Node)[0]
ACTOR2_ID = ret[0]._get_id()

ret = gdb.query(q="""START run=node(%d) MATCH run-[:HAS_TIMELINE]->()-[y:NEXT_LEVEL]->()-[m:NEXT_LEVEL]->()-[d:NEXT_LEVEL]->()-[h:NEXT_LEVEL]->hour WHERE d.day = 29 and h.hour = 10
RETURN hour""" % RUN_ID, returns=client.Node)[0]
HOUR_ID = ret[0]._get_id()


# =========================================

QUERY1 = """
START root = node(0)
MATCH root-[:HAS_RUN]->run-[:HAS_TIMELINE]->()-[y:NEXT_LEVEL]->()-[m:NEXT_LEVEL]->()-[d:NEXT_LEVEL]->()-[h:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]->frame
WHERE run.name="%s" and y.year=2009 and m.month=7 and d.day=1 and h.hour>=9 and h.hour<13
RETURN frame ORDER BY frame.timestamp
""" % RUN_NAME


QUERY2 = """
START frame = node(%d)
MATCH frame-[:FRAME_ACTOR]-actor
RETURN actor.name
""" % FRAME_ID


QUERY3 = """
START frame = node(%s)
MATCH frame-[r:FRAME_INTERACTION]-interaction
WHERE r.weight > %d 
RETURN interaction.actor1, interaction.actor2, r.weight;
""" % (FRAME_ID, 0)


QUERY4 = """
START run = node(%d)
MATCH run-[:RUN_ACTOR]->actor<-[r:FRAME_ACTOR]-()
RETURN actor.name, count(r)
""" % RUN_ID


QUERY5 = """
START run = node(%d)
MATCH run-[:RUN_ACTOR]->actor<-[r:FRAME_ACTOR]-()
WITH actor.name as name, COUNT(r) as freq
WHERE freq > 1000
RETURN name, freq ORDER BY freq DESC
""" % RUN_ID


QUERY5b = """
START run = node(%d)
MATCH run-[:RUN_ACTOR]-actor
WITH actor
MATCH ()-[r:FRAME_ACTOR]-actor
WITH actor.name as name, COUNT(r) as freq
WHERE freq > 1000
RETURN name, freq ORDER BY freq DESC;
""" % RUN_ID


QUERY6 = """
START actor = node(%d)
MATCH ()-[d:NEXT_LEVEL]->()-[:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-()-[:FRAME_ACTOR]-actor
RETURN DISTINCT(d.day)
""" % ACTOR_ID


QUERY6b = """
START actor = node(%d)
MATCH frame-[:FRAME_ACTOR]-actor
RETURN DISTINCT(frame.day)
""" % ACTOR_ID


QUERY7 = """
START actor1 = node(%d)
MATCH actor1<-[:INTERACTION_ACTOR]-()-[:INTERACTION_ACTOR]->actor2
RETURN actor2.name ORDER BY actor2.name
""" % ACTOR_ID


QUERY8 = """
START actor1 = node(%d)
MATCH actor1<-[:INTERACTION_ACTOR]-interaction-[:INTERACTION_ACTOR]->actor2
WITH interaction, actor2
MATCH ()-[d:NEXT_LEVEL]->()-[:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-()-[:FRAME_INTERACTION]-interaction
WHERE d.day = 7
RETURN DISTINCT(actor2.name)
""" % ACTOR_ID


QUERY9 = """
START actor1 = node(%d), actor2 = node(%d)
MATCH actor1<-[:INTERACTION_ACTOR]-()-[:INTERACTION_ACTOR]->actor
WITH COLLECT(actor) as neighs1, actor2
MATCH actor2<-[:INTERACTION_ACTOR]-()-[:INTERACTION_ACTOR]->actor
WHERE actor IN neighs1
RETURN actor
""" % (ACTOR1_ID, ACTOR2_ID)


QUERY9b = """
START actor1 = node(%d), actor2 = node(%d)
MATCH actor1<-[:INTERACTION_ACTOR]-()-[:INTERACTION_ACTOR]->actor<-[:INTERACTION_ACTOR]-()-[:INTERACTION_ACTOR]->actor2
RETURN actor
""" % (ACTOR1_ID, ACTOR2_ID)


QUERY10 = """
START run = node(%d)
MATCH run-[:RUN_ACTOR]-actor-[r:INTERACTION_ACTOR]-()
RETURN actor.name, COUNT(r) ORDER BY COUNT(r) DESC
""" % RUN_ID


QUERY11a = """
START actor = node(%d)
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction1-[:INTERACTION_ACTOR]->actor,
      ()-[d1:NEXT_LEVEL]->()-[h1:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_INTERACTION]-interaction1
WHERE d1.day = 29 and h1.hour = 10
WITH DISTINCT neigh1, actor
MATCH neigh2<-[:INTERACTION_ACTOR]-interaction2-[:INTERACTION_ACTOR]->actor,
      ()-[d2:NEXT_LEVEL]->()-[h2:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_INTERACTION]-interaction2
WHERE d2.day = 29 and h2.hour = 10
WITH distinct neigh2, neigh1
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction3-[:INTERACTION_ACTOR]->neigh2,
      ()-[d3:NEXT_LEVEL]->()-[h3:NEXT_LEVEL]->()-[:TIMELINE_INSTANCE]-frame-[:FRAME_INTERACTION]-interaction3
WHERE d3.day = 29 and h3.hour = 10
RETURN DISTINCT neigh1.actor, neigh2.actor ORDER BY neigh1.actor, neigh2.actor;
""" % ACTOR_ID


QUERY11b = """
START actor = node(%d), hour = node(%d)
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction1-[:INTERACTION_ACTOR]->actor,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_INTERACTION]->interaction1
WITH DISTINCT hour, neigh1, actor
MATCH neigh2<-[:INTERACTION_ACTOR]-interaction2-[:INTERACTION_ACTOR]->actor,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_INTERACTION]->interaction2
WITH DISTINCT hour, neigh1, neigh2
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction3-[:INTERACTION_ACTOR]->neigh2,
      hour-[:TIMELINE_INSTANCE]->frame-[:FRAME_INTERACTION]->interaction3
RETURN DISTINCT neigh1.actor, neigh2.actor ORDER BY neigh1.actor, neigh2.actor;
""" % (ACTOR_ID, HOUR_ID)


QUERY11c = """
START actor = node(%d)
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction1-[:INTERACTION_ACTOR]->actor,
      frame1-[:FRAME_INTERACTION]->interaction1
WHERE frame1.day = 29 and frame1.hour = 10
WITH DISTINCT neigh1, actor
MATCH neigh2<-[:INTERACTION_ACTOR]-interaction2-[:INTERACTION_ACTOR]->actor,
      frame2-[:FRAME_INTERACTION]->interaction2
WHERE frame2.day = 29 and frame2.hour = 10
WITH DISTINCT neigh1, neigh2
MATCH neigh1<-[:INTERACTION_ACTOR]-interaction3-[:INTERACTION_ACTOR]->neigh2,
      frame3-[:FRAME_INTERACTION]->interaction3
WHERE frame3.day = 29 and frame3.hour = 10
RETURN DISTINCT neigh1.actor, neigh2.actor ORDER BY neigh1.actor, neigh2.actor;
""" % ACTOR_ID



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

