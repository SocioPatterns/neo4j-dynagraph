#!/usr/bin/env python
#
# Parse a dynamic GEXF file
# and upload it to a Neo4j REST server
#
# LIMITATIONS:
# - does not parse node/edge attributes or other metadata
# - supports spells only -- not slices for dynamic attributes
# - only 'integer' timeformat is supported. Time is assumed to be POSIX time.
#
# Copyright (C) 2012 ISI Foundation
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
import argparse
import xml.etree.ElementTree as xml
from neo4jrestclient.client import GraphDatabase

parser = argparse.ArgumentParser(description='Load a dynamic GEXF file into a Neo4j REST store.')

parser.add_argument('gexf', metavar='<GEXF file>', type=argparse.FileType('r'), \
                   default=sys.stdin, help='dynamic GEXF input file')

parser.add_argument('name', metavar='<run name>', \
                   help='name of top-level "RUN" node of the Neo4J representation')

parser.add_argument('tstart', metavar='<start time>', type=int, \
                   help='start time for loading GEXF data')

parser.add_argument('delta', metavar='<frame duration>', type=int, \
                   default=20, help='duration in seconds of time frames')

parser.add_argument('neo4j', metavar='<Neo4j URL>', \
                   default="http://localhost:7474/db/data/", help='URL of Neo4j REST endpoint')

args = parser.parse_args()

GEXF_FILE = args.gexf
RUN_NAME = args.name
START_TIME = args.tstart
DELTAT = args.delta
NEO4J_REST = args.neo4j

# -----------------------------------------------------

gexf = xml.parse(GEXF_FILE).getroot()

graph = gexf.find('{http://www.gexf.net/1.2draft}graph')
if graph.get('mode') != "dynamic":
    sys.exit("GEXF file is not dynamic")
if graph.get('timeformat') != "integer":
    sys.exit('GEXF file does not have an "integer" timeformat')

def get_intervals(tstart, tstop):
    delta = (tstart - START_TIME) % DELTAT
    return [(t, t+DELTAT) for t in range(tstart-delta, tstop, DELTAT)]

NODE_TIMELINE = {}
nodes = graph.find('{http://www.gexf.net/1.2draft}nodes')

for node in nodes.findall('{http://www.gexf.net/1.2draft}node'):
    node_id = int(node.get('id'))
    if not node_id in NODE_TIMELINE:
        NODE_TIMELINE[node_id] = set()
    for spell in node.findall('{http://www.gexf.net/1.2draft}spells/{http://www.gexf.net/1.2draft}spell'):
        t1, t2 = int(spell.get('start')), int(spell.get('end'))
        NODE_TIMELINE[node_id].update( get_intervals(t1, t2) )

EDGE_TIMELINE = {}
edges = graph.find('{http://www.gexf.net/1.2draft}edges')

for edge in edges.findall('{http://www.gexf.net/1.2draft}edge'):
    node1, node2 = int(edge.get('source')), int(edge.get('target'))
    if not (node1,node2) in EDGE_TIMELINE:
        EDGE_TIMELINE[(node1,node2)] = set()
    for spell in edge.findall('{http://www.gexf.net/1.2draft}spells/{http://www.gexf.net/1.2draft}spell'):
        t1, t2 = int(spell.get('start')), int(spell.get('end'))
        EDGE_TIMELINE[(node1,node2)].update( get_intervals(t1, t2) )

FRAMES = set()
for interval_list in NODE_TIMELINE.values() + EDGE_TIMELINE.values():
    FRAMES.update(interval_list)
STOP_TIME = max( [ t2 for (t1,t2) in FRAMES] )

# -----------------------------------------------------

TLINE_DICT = {}

def add_to_timeline(root_node, node, timestamp):
    (year, month, day, hour, minute, second) = time.localtime(timestamp)[:6]

    if year in TLINE_DICT:
        (root_node, tline) = TLINE_DICT[year]
    else:
        TLINE_DICT[year] = (gdb.node(type="TIMELINE"), {})
        root_node.relationships.create("NEXT_LEVEL", TLINE_DICT[year][0], year=year)
        (root_node, tline) = TLINE_DICT[year]

    if month in tline:
        (root_node, tline) = tline[month]
    else:
        tline[month] = (gdb.node(type="TIMELINE"), {})
        root_node.relationships.create("NEXT_LEVEL", tline[month][0], month=month)
        (root_node, tline) = tline[month]
    
    if day in tline:
        (root_node, tline) = tline[day]
    else:
        tline[day] = (gdb.node(type="TIMELINE"), {})
        root_node.relationships.create("NEXT_LEVEL", tline[day][0], day=day)
        (root_node, tline) = tline[day]
    
    if hour in tline:
        (root_node, tline) = tline[hour]
    else:
        tline[hour] = (gdb.node(type="TIMELINE"), {})
        root_node.relationships.create("NEXT_LEVEL", tline[hour][0], hour=hour)
        (root_node, tline) = tline[hour]

    root_node.relationships.create("TIMELINE_INSTANCE", node, timestamp=timestamp)
    
    node['year'] = year
    node['month'] = month
    node['day'] = day
    node['hour'] = hour
    node['minute'] = minute
    node['second'] = second

# -----------------------------------------------------

gdb = GraphDatabase(NEO4J_REST)

actorsidx = gdb.nodes.indexes.create(name="actors_%s" % RUN_NAME, type="fulltext")

REF_NODE = gdb.node[0]
RUN = gdb.node(name=RUN_NAME, type='RUN')
REF_NODE.relationships.create("HAS_RUN", RUN)

TLINE = gdb.node(name='TIMELINE', type='TIMELINE', start=START_TIME, stop=STOP_TIME)
RUN.relationships.create("HAS_TIMELINE", TLINE)

ACTOR_DICT = {}
INTERACTION_DICT = {}

frame_count = 0
prev_frame = None

actors = set()
interactions = set()
frame_actors = []
frame_interactions = []

tx = gdb.transaction()

for frame_time in range(START_TIME, STOP_TIME, DELTAT):
    frame_count += 1
    if frame_count % 1000 == 0:
        tx.commit()
        tx = gdb.transaction()
     
    interval = (frame_time, frame_time+DELTAT)
    print '#%d' % frame_count, time.ctime(frame_time)

    frame = gdb.node(name='FRAME_%05d' % frame_count, type='FRAME', frame_id=frame_count, timestamp=frame_time,  timestamp_end=frame_time+DELTAT, time=time.ctime(frame_time), length=DELTAT)
    RUN.relationships.create("RUN_FRAME", frame)
    add_to_timeline(TLINE, frame, frame_time)
    
    if frame_count == 1:
        RUN.relationships.create("RUN_FRAME_FIRST", frame)

    if prev_frame:
        prev_frame.relationships.create("FRAME_NEXT", frame)
    prev_frame = frame

    for actor_id in NODE_TIMELINE:
        if not interval in NODE_TIMELINE[actor_id]:
            continue
        actors.add(actor_id)
        
        frame_actors.append((frame, actor_id))

    for (id1, id2) in EDGE_TIMELINE:
        if not interval in EDGE_TIMELINE[(id1,id2)]:
            continue

        if id1 > id2:
            (id1, id2) = (id2, id1)
            
        interactions.add((id1,id2))
        
        frame_interactions.append((frame, (id1,id2)))

tx.commit()

with gdb.transaction():
    print 'Adding %d ACTOR nodes' % len(actors)
    for actor_id in actors:
        actor = gdb.node(name='ACTOR_%04d' % actor_id, type='ACTOR', actor=actor_id)
        actorsidx.add('actor_id', actor_id, actor)
        ACTOR_DICT[actor_id] = actor
        RUN.relationships.create("RUN_ACTOR", actor)

    print 'Adding %d INTERACTION nodes' % len(interactions)
    for (id1,id2) in interactions:
        edge = gdb.node(name='INTERACTION_%04d_%04d' % (id1, id2), type='INTERACTION', actor1=id1, actor2=id2)
        INTERACTION_DICT[(id1,id2)] = edge
        actor1 = ACTOR_DICT[id1]
        actor2 = ACTOR_DICT[id2]
        edge.relationships.create("INTERACTION_ACTOR", actor1)
        edge.relationships.create("INTERACTION_ACTOR", actor2)
        RUN.relationships.create("RUN_INTERACTION", edge)

tx = gdb.transaction(update=False)
print 'Adding %d ACTOR relations to frames' % len(frame_actors)
for i, (frame, actor_id) in enumerate(frame_actors):
    if (i+i) % 1000 == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
        tx.commit()
        tx = gdb.transaction(update=False)
    frame.relationships.create("FRAME_ACTOR", ACTOR_DICT[actor_id])
tx.commit()
print

tx = gdb.transaction(update=False)
print 'Adding %d INTERACTION relations to frames' % len(frame_interactions)
for i, (frame, interaction) in enumerate(frame_interactions):
    if (i+1) % 1000 == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
        tx.commit()
        tx = gdb.transaction(update=False)
    frame.relationships.create("FRAME_INTERACTION", INTERACTION_DICT[interaction], weight=1)
tx.commit()
print

print 'Done.'

