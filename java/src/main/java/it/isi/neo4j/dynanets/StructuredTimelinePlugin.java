/**
 * Copyright (C) 2008-2010 Istituto per l'Interscambio Scientifico I.S.I.
 * You can contact us by email (isi@isi.it) or write to:
 * ISI Foundation, Viale S. Severo 65, 10133 Torino, Italy. 
 *
 * This program was written by André Panisson <panisson@gmail.com>
 *
 */
package it.isi.neo4j.dynanets;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

@Description( "An extension to the Neo4j Server for accessing the structured timeline" )
public class StructuredTimelinePlugin extends ServerPlugin
{
    
    @Name("create_timeline")
    @Description("")
    @PluginTarget( GraphDatabaseService.class )
	public Node createTimeline(
			@Source GraphDatabaseService graphDb,
			@Description("The node that will represent the timeline.") @Parameter(name = "tnode") Node tnode,
			@Description("The timeline name.") @Parameter(name = "name") String name) {
    	new StructuredTimeline( name, tnode, graphDb );
    	return tnode;
    }
    
    @Name("add_timeline_node")
    @Description("")
    @PluginTarget( GraphDatabaseService.class )
	public Node addTimelineNode(
			@Source GraphDatabaseService graphDb,
			@Description("The node to add to timeline.") @Parameter(name = "node") Node node,
			@Description("The node representing the timeline.") @Parameter(name = "tnode") Node tnode,
			@Description("The timestamp.") @Parameter(name = "timestamp") Long timestamp) {
    	
    	Transaction tx = graphDb.beginTx();
		try {
	    	String timelineName = tnode.getProperty( "timeline_name" ).toString();
	    	StructuredTimeline timeline = new StructuredTimeline( timelineName, tnode, graphDb );
	    	timeline.addNode(node, timestamp);
	    	tx.success();
		} finally {
			tx.finish();
		}
		
		return node;
    }
    
    @Name("get_timeline_nodes")
    @Description("")
    @PluginTarget( GraphDatabaseService.class )
	public Iterable<Node> getTimelineNodes(
			@Source GraphDatabaseService graphDb,
			@Description("The node representing the timeline.") @Parameter(name = "tnode") Node tnode,
			@Description("The timestamp.") @Parameter(name = "timestamp") Long timestamp) {
    	String timelineName = tnode.getProperty( "timeline_name" ).toString();
    	StructuredTimeline timeline = new StructuredTimeline( timelineName, tnode, graphDb );
    	return timeline.getNodes(timestamp);
    }
    
    @Name("get_timeline_nodes_by_date")
    @Description("")
    @PluginTarget( GraphDatabaseService.class )
	public Iterable<Node> getTimelineNodesByDate(
			@Source GraphDatabaseService graphDb,
			@Description("The node representing the timeline.") @Parameter(name = "tnode") Node tnode,
			@Description("The date.") @Parameter(name = "date") String date) {
		try {
			Date d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date);
			return this.getTimelineNodes(graphDb, tnode, d.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
}