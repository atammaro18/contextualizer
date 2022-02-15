from flask import Flask
from flask_restful import Api
from collections import defaultdict                                                                 # import dictionary for graph
import threading                                                                                    # import module so we can properly loop and terminate the loop of createGraph
import sys                                                                                          # import for terminating the program
import sqlite3                                                                                      # import for SQL commands to get tag data for a SQL database
import glob                                                                                         # used to find the newest database file for OPC data
import os                                                                                           # used to get OS information


app = Flask(__name__)
api = Api(app)

global create_graph                                                                                 # so we can properly end the loop and quit the program upon termination
create_graph = threading.Event()                                                                    # create a threading event for creating the graph (allows us to refresh the graph's tag data while other processes run)

@app.route('/graph', methods = ['GET'])
@app.route('/graph/<tag_node>', methods = ['GET'])
def createGraph():
    global graph
    graph = defaultdict(list)                                                                       # create a dict for the graph
    
    #db_file_list = glob.glob('C:\VS Code\OPC_UA_Databases\*.db')                                    # for the file path in the string, store a list of every file
    db_file_list = glob.glob('C:\\Users\\atammaro\\Desktop\\Stefanini\\*.db')
    latest_db = max(db_file_list, key=os.path.getctime)                                             # find the newest file in the list

    conn = sqlite3.connect(latest_db)                                                               # connect to the SQL database
    c = conn.cursor()                                                                               # create a cursor for the database

    c.execute('SELECT IP FROM i')                                                                   # select ip column
    tag_ip = c.fetchall()                                                                           # store a tuple of all ip's
    tag_ip = list(tag_ip)                                                                           # covert the tuple to a list

    c.execute('SELECT TREE_NODE FROM i')                                                              # select node column from the database
    tag_node = c.fetchall()                                                                         # store a tuple of all node locations for tags
    tag_node = list(tag_node)                                                                       # convert the tuple to a list

    c.execute('SELECT BRANCH_NODE FROM i')                                                          # select the branch node column of the database
    tag_branch_node = c.fetchall()                                                                  # store a tuple of all branch nodes from the database
    tag_branch_node = list(tag_branch_node)                                                         # covert the tuple to a list

    c.execute('SELECT BRANCH_NODE_VAL FROM i')                                                      # select the branch node value column from the database
    tag_branch_node_value = c.fetchall()                                                            # store a tuple of all branch node values
    tag_branch_node_value = list(tag_branch_node_value)                                             # covert the tuple to a list

    c.execute('SELECT BRANCH_NODE_TYPE FROM i')                                                     # select all branch node data types from the list
    tag_branch_node_type = c.fetchall()                                                             # store a tuple of all branch node data types
    tag_branch_node_type = list(tag_branch_node_type)                                               # covert the tuple to a list

    c.execute('SELECT TIME_RECEIVED FROM i')                                                        # select the column of times we recieved the tag data
    time_recieved = c.fetchall()                                                                    # store a tuple of the times
    time_recieved = list(time_recieved)                                                             # covert the tuple to a list

    global total_tag_count                                                                          # total number tags from the database
    global tag_field_count                                                                          # number of columns for each tag including the tag name
    total_tag_count = len(tag_ip)
    tag_field_count = 6                                                                             

    for x in range(0, total_tag_count):                                                             # for each tag in the database        
        graph[x] = []                                                                               # make an entry in the graph for each tag
        tag_ip[x] = list(tag_ip[x])                                                                 # convert each ip from tuple to list
        tag_node[x] = list(tag_node[x])                                                             # convert each node from tuple to list 
        tag_branch_node[x] = list(tag_branch_node[x])                                               # convert each branch node from tuple to list
        tag_branch_node_value[x] = list(tag_branch_node_value[x])                                   # covert all data from tuple to list
        tag_branch_node_type[x] = list(tag_branch_node_type[x])                                     # covert data types from tuple to list
        time_recieved[x] = list(time_recieved[x])                                                   # covert times from tuple to list
        for y in range(0, tag_field_count):
            graph[x][y] = graph[x].append('\0')                                                     # initialize the graph so we can insert tag data directly into the correct location by placing a blank character in each spot we need

    for x in range(0, total_tag_count):                                                             # move each piece of tag data from its respective list to the graph
        graph[x][0] = tag_ip[x][0]                                                                  # first item in graph entry is tag ip
        graph[x][1] = tag_node[x][0]                                                                # second item in the graph entry is the tag node
        graph[x][2] = tag_branch_node[x][0]                                                         # thirf item in the graph entry is the branch node
        graph[x][3] = tag_branch_node_value[x][0]                                                   # fourth item is data
        graph[x][4] = tag_branch_node_type[x][0]                                                    # fifth item is data type
        graph[x][5] = time_recieved[x][0]                                                           # sixth is time recieved for the data
    
    return graph

def getUserInputs():
    connections = '\0'                                                                              # initialize connections input variable
    connections = input("\033[1;32mPlease enter the number of connections in the system:\033[0;37m ")           # user enters the number of connections in their system
    connections = int(connections)                                                                  # convert the string containing the number they entered to an int
    print("\n")

    for x in range(0, connections):                                                                 # for each connection (edge) in the system (graph)
        node_name_src = input("\033[1;32mEnter the tag name of a source node:\033[0;37m ")          # user enters the tag name for a source node of the graph
        node_name_dest = input("\033[1;32mEnter the tag name of a destination node:\033[0;37m ")    # user enters the tag name for the dest node of that source node
        for y in range(0, total_tag_count):                                                         # for each tag in the graph
            if node_name_dest == graph[y][0]:                                                       # make sure then destination node entered exists
                dest_found = True                                                                   # flag that the dest that was entered exists and can be added to the node
                break                                                                               # if we find it, break from the for loop
            else:
                dest_found = False                                                                  # if dest doesn't exist, flag that it wasn't found so it isn't added to the node and we can flag an error message
        for y in range(0, total_tag_count): 
            if dest_found == True:                                                                  # add correctly entered dest node to the graph and node y, otherwise just skip this since it dosn't matter
                if node_name_src == graph[y][0]:                                                    # ...if the source node tag name also matches a tag name 
                    src_found = True                                                                # flag that we found the source node correctly
                    graph[y].append(node_name_dest)                                                 # append the dest node tag name to the source graph node's list
                    break                                                                           # break so we can get a new connection
                else:
                    src_found = False                                                               # if the src tag entered doesn't exist, flag it so we can throw an error message
        if dest_found == False or src_found == False:
            print("\033[1;31mInvalid node entered! Connection ignored!\033[0;37m")
        print("")

def printSystemAndNodes():
    #print("\n\n\033[1;31mList of System Connections (Source Node -> Destination Node):\033[0;37m")
    #for x in range(0, total_tag_count):
    #    for y in range(tag_field_count, len(graph[x])):                                             # (Source Node Tag Name -> Dest Node Tag Name)
    #        print(end = "\033[1;36m")
    #        print(graph[x][0], "->", graph[x][y])                                                   # print the edges of the graph by printing graph[x][0] (source node tag name) followed by each list item...
    #        print(end = "\033[0;37m")                                                               # ...for node x starting at graph[x][5] (the sixth list item, after the tag data, if it exists)
    #print("\n")       
                                                                                
    print("\033[1;31mList of Graph Data:\033[0;37m")
    for x in range(0, total_tag_count):
        print("\033[1;32mGraph Node", x, ":\033[0;37m")
        for y in range(0, len(graph[x])):
            if y == 0:
                print("\033[1;36mTag IP:", graph[x][y],"\033[0;37m")                                # print the first list item (ip) for graph entry x (node x)
            if y == 1:
                print("\033[1;36mTag Tree Node:", graph[x][y], end = "\033[0;37m\n")                # print the second list item (tag tree node number) for graph entry x (node x)
            if y == 2:
                print("\033[1;36mBranch Node:", graph[x][y], end = "\033[0;37m\n")                  # print the third list item (branch node number) for graph entry x (node x)
            if y == 3:
                print("\033[1;36mBranch Node Value:", graph[x][y], end = "\033[0;37m\n")            # print the fourth list item  (branch node value) for graph entry x (node x)
            if y == 4:
                print("\033[1;36mBranch Node Data Type:", graph[x][y], end = "\033[0;37m\n")        # print the fifth  list item (data type) for graph entry x (node x)
            if y == 5:
                print("\033[1;36mTime Data Recieved:", graph[x][y], end = "\033[0;37m\n")           # print the sixth list item (time data recieved) for graph entry x (node x)
            if y == tag_field_count:
                print("\033[1;36mDestination Node Tags:", end = "")                                 # print a list of all dest nodes related to the tag
                for y in range (tag_field_count, len(graph[x])):
                    print(" ", graph[x][y], end = "")
                print("\033[0;37m")
        print("")

def storeNodesForReprint():
    global store_src_dest_nodes                                                                     
    store_src_dest_nodes = defaultdict(list)                                                        # dict for storing the source tag names and all of their dest nodes

    for x in range(0, total_tag_count):
        store_src_dest_nodes[x] = []                                                                # initialize the node dict
        store_src_dest_nodes[x].append(graph[x][0])                                                 # append the source node tag name to the beginning of each list in the node dict

    for x in range(0, total_tag_count):
        if len(graph[x]) > 4:                                                                       # if there are any destination nodes for a node
            for y in range(5, len(graph[x])):
                store_src_dest_nodes[x].append(graph[x][y])                                         # for each tag, check if there are any dest nodes in the graph for it and store them in the node dict if there are

def reinsertNodes():
    # this is the loop that will check if a connection still exists by checking each connection's source and dest tag name, and reinsert it into the graph if it does
    for w in range(0, len(store_src_dest_nodes)):                                                   # for each source node from the connections that existed before the graph was refreshed for reprinting
        for x in range(0, total_tag_count):                                                         # for each tag (node) in the refreshed graph
            if store_src_dest_nodes[w][0] == graph[x][0]:                                           # keep looping until we find where the source node tag name in the graph and stored node dicts match, or skip to the next source node if none match
                # source node found
                for y in range (1, len(store_src_dest_nodes[w])):                                   # once we find the source node, for each dest node in the stored node dict
                    for z in range(0, total_tag_count):                                             # for each node's tag name in the graph
                        if store_src_dest_nodes[w][y] == graph[z][0]:                               # check to make sure that the dest node(s) for a source node still exist too
                            # dest node y found at graph entry z, for source node w at graph entry x, so append it to source node graph entry x
                            graph[x].append(store_src_dest_nodes[w][y])                             # if the source node AND dest node still exist with their original tag names, reinsert them into the graph as they were before

def addConnection():
    connections = '\0'                                                                              # initialize connections input variable
    connections = input("\n\033[1;32mPlease enter the number of connections to be added to the system:\033[0;37m ")           # user enters the number of connections in their system
    connections = int(connections)                                                                  # convert the string containing the number they entered to an int
    print("\n")

    for x in range(0, connections):                                                                 # for each connection (edge) in the system (graph)
        node_name_src = input("\033[1;32mEnter the tag name of a source node:\033[0;37m ")          # user enters the tag name for a source node of the graph
        node_name_dest = input("\033[1;32mEnter the tag name of a destination node:\033[0;37m ")    # user enters the tag name for the dest node of that source node
        for y in range(0, total_tag_count):                                                         # for each tag in the graph
            if node_name_dest == graph[y][0]:                                                       # make sure then destination node entered exists
                dest_found = True                                                                   # flag that the dest that was entered exists and can be added to the node
                break                                                                               # if we find it, break from the for loop
            else:
                dest_found = False                                                                  # if dest doesn't exist, flag that it wasn't found so it isn't added to the node and we can flag an error message
        for y in range(0, total_tag_count): 
            if dest_found == True:                                                                  # add correctly entered dest node to the graph and node y, otherwise just skip this since it dosn't matter
                if node_name_src == graph[y][0]:                                                    # ...if the source node tag name also matches a tag name 
                    src_found = True                                                                # flag that we found the source node correctly
                    graph[y].append(node_name_dest)                                                 # append the dest node tag name to the source graph node's list
                    break                                                                           # break so we can get a new connection
                else:
                    src_found = False                                                               # if the src tag entered doesn't exist, flag it so we can throw an error message
        if dest_found == False or src_found == False:
            print("\033[1;31mInvalid node entered! Connection ignored!\033[0;37m")

def removeConnection():
    connections = '\0'                                                                              # initialize connections input variable
    connections = input("\n\033[1;32mPlease enter the number of connections to be removed from the system:\033[0;37m ")           # user enters the number of connections in their system
    connections = int(connections)                                                                  # convert the string containing the number they entered to an int
    print("\n")

    for x in range(0, connections):                                                                 # for each connection (edge) in the system (graph)
        node_name_src = input("\033[1;32mEnter the tag name of a source node:\033[0;37m ")          # user enters the tag name for a source node of the graph
        node_name_dest = input("\033[1;32mEnter the tag name of a destination node:\033[0;37m ")    # user enters the tag name for the dest node of that source node
        for y in range(0, total_tag_count):                                                         # for each tag in the graph
            if node_name_dest == graph[y][0]:                                                       # make sure then destination node entered exists
                dest_found = True                                                                   # flag that the dest that was entered exists and can be added to the node
                break                                                                               # if we find it, break from the for loop
            else:
                dest_found = False                                                                  # if dest doesn't exist, flag that it wasn't found so it isn't added to the node and we can flag an error message
        for y in range(0, total_tag_count): 
            if dest_found == True:                                                                  # add correctly entered dest node to the graph and node y, otherwise just skip this since it dosn't matter
                if node_name_src == graph[y][0]:                                                    # ...if the source node tag name also matches a tag name 
                    src_found = True                                                                # flag that we found the source node correctly
                    for z in range(5, len(graph[y])):                                               # for each connection a source node has
                        if graph[y][z] == node_name_dest:                                           # find the dest node we want to delete
                            del graph[y][z]                                                         # delete it
                            break                                                                   # break so we stop looking for dest nodes
                    break                                                                           # break so we can get a new connection
                else:
                    src_found = False                                                               # if the src tag entered doesn't exist, flag it so we can throw an error message
        if dest_found == False or src_found == False:
            print("\033[1;31mInvalid node entered! Connection ignored!\033[0;37m")

def mainStart():
        createGraph()

        timer_graph = threading.Timer(1.0, createGraph)                                                 # timer to refresh graph data periodically
        timer_graph.start()                                                                             # start the timer

        # getUserInputs()                                                                               # Removing this from runtime for now for ease of use, might add back later if we have a use for it
        printSystemAndNodes()

def mainLoop():
    while True:
        print("\n\033[1;31mEnter One of the Following Inputs to Continue:\n")
        print("\033[1;32mEnter: Refresh and Re-Print Graph Data")
        # print("\033[1;32m'Add': Add a Connection to Existing Connections")
        # print("\033[1;32m'Remove': Remove a Connection from Existing Connections")
        # print("\033[1;32m'Reset': Wipe Connections, Enter a New Set of Connections, Refresh Data, and Re-Print")
        print("\033[1;32m'Q': End the Program\033[0;37m\n")
        
        user_edit_nodes = input()
        
        #if user_edit_nodes.casefold() == "add":
        #    addConnection()                                                                         # add however many connections the user wants to add to the graph
        #    printSystemAndNodes()                                                                   # reprint graph with most recent data and new connections

        #if user_edit_nodes.casefold() == "remove":
        #    removeConnection()                                                                      # delete however many connections the user wants to delete from the graph
        #    printSystemAndNodes()                                                                   # reprint graph with most recent data and new connections
        
        #if user_edit_nodes.casefold() == "reset":                                                   # if the user wants to reset their connections
        #    print("\n")
        #    graph.clear()                                                                           # completely remove everything from the graph
        #    createGraph()                                                                           # update the graph's node data
        #    # getUserInputs()                                                                       # get new connections from the user, removing this from runtime for now for ease of use, might add back later if we have a use for it
        #    printSystemAndNodes()                                                                   # reprint graph with most recent data and new connections

        if user_edit_nodes.casefold() == "q":                                                       # if the user enters q or Q
            create_graph.clear()                                                                    # stop graph update loop
            sys.exit("\033[1;31m\n\nProgram Ended!\033[0;37m")                                      # end the program
        
        
        if user_edit_nodes.casefold() == "":                                                        # if the user ONLY pressed enter, with no other inputs i.e. space or letters
            # if the user is just reprinting the graph, we want to completely recreate the graph using all of the newest data in the database...
            # ...to do that we need to store all of the user's connections before we wipe the graph. we want to reprint the graph like this in case there are any tags...
            # ...that were added or removed. as long as the tag names for nodes with connections don't change the graph will reprint without issues even if there are new or deleted tags...
            # ...between reprints. if any tag names were changed, the user should enter "edit" anyway
            #storeNodesForReprint()                                                                  # store the user's connections before wiping the graph                   
            graph.clear()                                                                           # wipe the graph entirely
            createGraph()                                                                           # update the graph with most recent data from the CSV
            #reinsertNodes()                                                                         # reinsert the user's connections in the graph
            printSystemAndNodes()                                                                   # reprint the data lists with the original connections and updated data

        else:                                                                                       # if the user doesn't input what we want, prompt again
            continue

#api.add_resource(create_graph, '/tags/<string: name>')

if __name__ == "__main__":
     app.run(port=6800, debug = True)


mainStart()
mainLoop()

