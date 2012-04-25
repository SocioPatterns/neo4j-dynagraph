/**
 * Copyright (C) 2008-2010 Istituto per l'Interscambio Scientifico I.S.I.
 * You can contact us by email (isi@isi.it) or write to:
 * ISI Foundation, Viale S. Severo 65, 10133 Torino, Italy. 
 *
 * This program was written by André Panisson <panisson@gmail.com>
 *
 */
package it.isi.neo4j.dynanets;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class StructuredTimeline  extends BaseTimeline {

	static enum StructuredRelTypes implements RelationshipType
    {
		NEXT_LEVEL
    }
	
	private final GraphDatabaseService graphDb;
	
	public StructuredTimeline( String name, Node underlyingNode, GraphDatabaseService graphDb ) {
		super(name, underlyingNode, graphDb);
		this.graphDb = graphDb;
	}

	public void addNode(Node nodeToAdd, long timestamp) {
		super.addNode(nodeToAdd, timestamp);
		Calendar c = new GregorianCalendar();
		c.setTimeInMillis(timestamp*1000);
		
		Node underlyingNode = this.getUnderlyingNode();
		Node nextLevel = underlyingNode;
		
		nextLevel.setProperty("next_level", "year");
		nextLevel = createNextLevelNode(nextLevel, "year", c.get(Calendar.YEAR));
		nextLevel.setProperty("next_level", "month");
		nextLevel = createNextLevelNode(nextLevel, "month", c.get(Calendar.MONTH)+1);
		nextLevel.setProperty("next_level", "day");
		nextLevel = createNextLevelNode(nextLevel, "day", c.get(Calendar.DAY_OF_MONTH));
		nextLevel.setProperty("next_level", "hour");
		nextLevel = createNextLevelNode(nextLevel, "hour", c.get(Calendar.HOUR_OF_DAY));
		
		Relationship rel = nodeToAdd.getSingleRelationship(RelTypes.TIMELINE_INSTANCE, Direction.INCOMING);
		Node timeNode = rel.getStartNode();
		nextLevel.setProperty("next_level", "timestamp");
		rel = nextLevel.createRelationshipTo(timeNode, StructuredRelTypes.NEXT_LEVEL);
		rel.setProperty("timestamp", timestamp);
		
	}

	@Override
	public Iterable<Node> getNodes(long timestamp) {
		
		Calendar c = new GregorianCalendar();
		c.setTimeInMillis(timestamp*1000);
		List<Node> nodeList = new ArrayList<Node>();
		
		Node underlyingNode = this.getUnderlyingNode();
		Node currentNode = underlyingNode;
		
		currentNode = getNextLevelNode(currentNode, "year", c.get(Calendar.YEAR));
		if (currentNode == null) return nodeList;
		
		currentNode = getNextLevelNode(currentNode, "month", c.get(Calendar.MONTH)+1);
		if (currentNode == null) return nodeList;
		
		currentNode = getNextLevelNode(currentNode, "day", c.get(Calendar.DAY_OF_MONTH));
		if (currentNode == null) return nodeList;
		
		currentNode = getNextLevelNode(currentNode, "hour", c.get(Calendar.HOUR_OF_DAY));
		if (currentNode == null) return nodeList;
		
		currentNode = getNextLevelNode(currentNode, "timestamp", timestamp);
		if (currentNode == null) return nodeList;
		
		do
        {
            long currentTime = (Long) currentNode.getProperty( "timestamp" );
            if ( currentTime == timestamp )
            {
                for ( Relationship instanceRel : currentNode.getRelationships(
                        RelTypes.TIMELINE_INSTANCE, Direction.OUTGOING ) )
                {
                    nodeList.add( instanceRel.getEndNode() );
                }
                break;
            }
            if ( currentTime > timestamp )
            {
                break;
            }
            Relationship rel = currentNode.getSingleRelationship(
                    RelTypes.TIMELINE_NEXT_ENTRY, Direction.OUTGOING );
            currentNode = rel.getEndNode();
        }
        while ( !currentNode.equals( underlyingNode ) );
		return nodeList;
	}
	
	private Node getNextLevelNode(Node parent, String propertyName, Object propertyValue) {
		Relationship rel = null;
		for (Relationship r: parent.getRelationships(Direction.OUTGOING, StructuredRelTypes.NEXT_LEVEL)) {
			if (r.getProperty(propertyName).equals(propertyValue)) {
				rel = r;
				break;
			}
		}
		
		return ( rel == null ) ? null : rel.getEndNode();
		
	}
	
	private Node createNextLevelNode(Node parent, String propertyName, Object propertyValue) {
		Node nextLevel = getNextLevelNode(parent, propertyName, propertyValue);
		
		if ( nextLevel == null )
        {
			nextLevel = graphDb.createNode();
			Relationship rel = parent.createRelationshipTo( nextLevel,
					StructuredRelTypes.NEXT_LEVEL );
			rel.setProperty(propertyName, propertyValue);
        }
		
		return nextLevel;
	}

}
