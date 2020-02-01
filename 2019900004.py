import os
import sys
import re as regex
import itertools
import numpy as np

metaFilesDir = "./files/"
coreMetaDataFile = "./files/metadata.txt"
literalsTag = "<literal>"
dataBaseSchema = {}

def retrieveQueryElements(queryTokens):
    fromIndex  = queryTokens.index("from")
    whereIndex = [idx for idx, t in enumerate(queryTokens) if t == "where"]

    columns = queryTokens[1:fromIndex]
    if len(whereIndex) != 0:
        tables = queryTokens[fromIndex + 1:whereIndex[0]]
        conditions = queryTokens[whereIndex[0] + 1:]
    else:
        tables = queryTokens[fromIndex + 1:]
        conditions = []

    if (len(tables) == 0) :
        print("Error : table name is missing, re-run the program with correct query")
        exit(-1)

    if (len(whereIndex) != 0 and len(conditions) == 0) :
        print("Error : bounding conditions are  missing, re-run the program with correct query")
        exit(-1)

    if (len(columns) == 0) :
        print("Error : column names are missing, re-run the program with correct query")
        exit(-1)

    return tables,columns,conditions


def getColumnConfig(columns, tables,aliasTableMapping):
    columnConfiguration = []
    columns = "".join(columns).split(",")
    for column in columns:
        regmatch = regex.match("(.+)\((.+)\)", column)
        if regmatch:
            aggregateFunction, column = regmatch.groups()
        else:
            aggregateFunction = None
        if "." in column and len(column.split(".")) != 2 :
            print("Error : Invalid column name")
            exit(-1)

        tableName = None
        if "." in column:
            tableName, columnName = column.split('.')
            if(tableName not in aliasTableMapping.keys()) :
                print("Error : table name not recognized ")
                exit(-1)
        else :
            columnName = column
            if columnName != "*":
                tableName = [t for t in tables if columnName in dataBaseSchema[aliasTableMapping[t]]]
                if(len(tableName)>1):
                    print("Error : field name is not unique '{}'".format(column))
                    exit(-1)
                if (len(tableName) == 0):
                    print("Error : Unknown column  name '{}'".format(column))
                    exit(-1)
                tableName = tableName[0]

        if columnName == "*":
            if(aggregateFunction != None):
                print("Error : Cant use Aggregate Function in this query ")
                exit(-1)
            if(tableName !=None) :
                columnConfiguration.extend([(tableName, c, aggregateFunction) for c in dataBaseSchema[aliasTableMapping[tableName]]])
            else:
                for t in tables:
                    columnConfiguration.extend([(t, c, aggregateFunction) for c in dataBaseSchema[aliasTableMapping[t]]])
        else:
            if columnName not  in dataBaseSchema[aliasTableMapping[tableName]] :
                print("Error : unknown field : '{}'".format(column))
                exit(-1)
            columnConfiguration.append((tableName, columnName, aggregateFunction))

    s = [a for t, c, a in columnConfiguration]
    if(all(s) ^ any(s)):
        print("Error : Cant use both aggregated and non-aggregated columns in single query")
        exit(-1)
    return columnConfiguration


def getAliasTableMapping(tablesArr):
    tables = []
    aliasTableMapping = {}
    tablesArr = " ".join(tablesArr).split(",")
    for table in tablesArr:
        t = table.split()
        if len(t) == 1:
            tb_name, tb_alias = t[0], t[0]
        else:
            tb_name, _, tb_alias = t

        if tb_name not in dataBaseSchema.keys():
            print("Error  :  no table name '{}'".format(tb_name))
            exit(-1)
        if tb_alias in aliasTableMapping.keys():
            print("Error : Table name is not unique '{}'".format(tb_alias))
            exit(-1)

        tables.append(tb_alias)
        aliasTableMapping[tb_alias] = tb_name
    return tables, aliasTableMapping


def initialize_data(data):
    tableName = None
    for tag in data:
        tag = tag.lower()
        if tag == "<begin_table>":
            attrs, tableName = [], None
        elif tag == "<end_table>":
            pass
        elif not tableName:
            tableName, dataBaseSchema[tag] = tag, []
        else:
            dataBaseSchema[tableName].append(tag)


def getConditionalMapping(conditions, tables, aliasTableMapping):
    filteredConditions = []
    conditionalMappings = None

    if conditions:
        conditions = " ".join(conditions)

        if " or " in conditions :
            conditionalMappings = "or"
        if " and " in conditions :
            conditionalMappings = "and"
        if conditionalMappings :
            conditions = conditions.split(conditionalMappings)
        else:
            conditions = [conditions]

        for cond in conditions:
            relate_op, left, right = getRelatedOperation(cond)
            parsed_cond = [relate_op]
            for idx, rc in enumerate([left, right]):
                try:
                    _ = int(rc)
                    flag = True
                except:
                    flag = False

                if flag:
                    parsed_cond.append((literalsTag, rc))
                    continue

                if "." in rc:
                    tname, cname = rc.split(".")
                else:
                    cname = rc
                    tname = [t for t in tables if rc in dataBaseSchema[aliasTableMapping[t]]]
                    tname = tname[0]
                parsed_cond.append((tname, cname))
            filteredConditions.append(parsed_cond)
    return filteredConditions, conditionalMappings

def getRelatedOperation(cond):
    if "<=" in cond:
        operation = "<="
    elif ">=" in cond:
        operation = ">="
    elif "<>" in cond:
        operation = "<>"
    elif ">" in cond:
        operation = ">"
    elif "<" in cond:
        operation = "<"
    elif "=" in cond:
        operation = "="
    else :
        if True:
            print("Error : Invalid condition ")
            exit(-1)
    if cond.count(operation) != 1:
        print("Error : Invalid condition ")
        exit(-1)
    l, r = cond.split(operation)
    l = l.strip()
    r = r.strip()
    return operation, l, r


def getTable(finalQueryDir):
    aliasTableMapping = finalQueryDir['aliasTableMapping']
    requiredColumnsToShow = finalQueryDir['requiredColumnsToShow']
    tables = finalQueryDir['tables']
    conditions = finalQueryDir['conditions']
    conditionalMappings = finalQueryDir['conditionalMappings']
    columnConfiguration = finalQueryDir['columnConfiguration']

    columnIndex = {}
    count = 0
    allTables = []

    for t in tables:
        lt = np.genfromtxt(os.path.join(metaFilesDir, "{}.csv".format(aliasTableMapping[t])), dtype=int, delimiter=',')

        idxs = [dataBaseSchema[aliasTableMapping[t]].index(cname) for cname in requiredColumnsToShow[t]]
        lt = lt[:, idxs]
        allTables.append(lt.tolist())

        columnIndex[t] = {cname: count + i for i, cname in enumerate(requiredColumnsToShow[t])}
        count += len(requiredColumnsToShow[t])

        # cartesian product of all tables
    inter_table = [[i for tup in r for i in list(tup)] for r in itertools.product(*allTables)]
    inter_table = np.array(inter_table)

    # check for conditions and get reduced table
    if len(conditions):
        totake = np.ones((inter_table.shape[0], len(conditions)), dtype=bool)

        for idx, (op, left, right) in enumerate(conditions):
            cols = []
            for tname, cname in [left, right]:
                if tname == literalsTag:
                    cols.append(np.full((inter_table.shape[0]), int(cname)))
                else:
                    cols.append(inter_table[:, columnIndex[tname][cname]])

            if op == "<=": totake[:, idx] = (cols[0] <= cols[1])
            if op == ">=": totake[:, idx] = (cols[0] >= cols[1])
            if op == "<>": totake[:, idx] = (cols[0] != cols[1])
            if op == "<": totake[:, idx] = (cols[0] < cols[1])
            if op == ">": totake[:, idx] = (cols[0] > cols[1])
            if op == "=": totake[:, idx] = (cols[0] == cols[1])

        if conditionalMappings == " or ":
            final_take = (totake[:, 0] | totake[:, 1])
        elif conditionalMappings == " and ":
            final_take = (totake[:, 0] & totake[:, 1])
        else:
            final_take = totake[:, 0]
        inter_table = inter_table[final_take]

    select_idxs = [columnIndex[tn][cn] for tn, cn, aggr in columnConfiguration]
    inter_table = inter_table[:, select_idxs]

    # process for aggregate function
    if columnConfiguration[0][2]:
        out_table = []
        disti = False
        for idx, (tn, cn, aggr) in enumerate(columnConfiguration):
            col = inter_table[:, idx]
            if aggr == "min":
                out_table.append(min(col))
            elif aggr == "max":
                out_table.append(max(col))
            elif aggr == "sum":
                out_table.append(sum(col))
            elif aggr == "average":
                out_table.append(sum(col) / col.shape[0])
            elif aggr == "count":
                out_table.append(col.shape[0])
            elif aggr == "distinct":
                seen = set()
                out_table = [x for x in col.tolist() if not (x in seen or seen.add(x))]
                disti = True
            else:
                print("Error : invalid aggregate")
                exit(-1)

        out_table = np.array([out_table])
        if disti: out_table = np.array(out_table).T
        out_header = ["{}({}.{})".format(aggr, tn, cn) for tn, cn, aggr in columnConfiguration]
    else:
        out_table = inter_table
        out_header = ["{}.{}".format(tn, cn) for tn, cn, aggr in columnConfiguration]
    return out_header, out_table.tolist()



def MiniSqlEngine(query):
    print("Welcome to Mini SQL Engine..!\n")
    print("please wait while system processing your query..!\n")

    with open(coreMetaDataFile, "r") as f:
        data = f.readlines()
    data = [t.strip() for t in data if t.strip()]

    initialize_data(data)
    queryTokens = query.split();
    reqTokens =["select","from"]
    flag = True
    if (all(x in queryTokens for x in reqTokens)):
        flag = False
    if (queryTokens[0] != "select") :
        flag = True
    # printing error
    if (flag):
        print("Error : query must have 'select' and 'from' tokens and 'select' must be included at start of query please re-run program with correct query")
        exit(-1)

    if(queryTokens.count("select") >1 or queryTokens.count("from")>1) :
        print("Error : select or from occured multiple times in given query please re-run program with correct query")
        exit(-1)

    # If we have all data and other fields start parsing query

    # extract and separate elements in query
    basicTables, columns, baseConditions = retrieveQueryElements(queryTokens)
  #  tables = " ".join(basicTables).split(",")
    tables, aliasTableMapping = getAliasTableMapping(basicTables)
    columnConfiguration = getColumnConfig(columns,tables,aliasTableMapping)
    conditions, conditionalMappings = getConditionalMapping(baseConditions, tables, aliasTableMapping)
  #  print("conditions data received")
  #  print(conditions)
  #  print(conditionalMappings)

    requiredColumnsToShow = {t: set() for t in tables}
    for tn, cn, _ in columnConfiguration: requiredColumnsToShow[tn].add(cn)
    for cond in conditions:
        for tn, cn in cond[1:]:
            if tn ==literalsTag: continue
            requiredColumnsToShow[tn].add(cn)

    for t in tables: requiredColumnsToShow[t] = list(requiredColumnsToShow[t])

    finalQueryDir =  {'tables':tables,'aliasTableMapping':aliasTableMapping,'columnConfiguration':columnConfiguration,'conditions':conditions,'conditionalMappings':conditionalMappings,'requiredColumnsToShow':requiredColumnsToShow}

    tableHeaders,tableValues = getTable(finalQueryDir)

    print("Query.. : "+ query+" \n")
    print("Answer.......... \n")
	
    print(",".join(map(str, tableHeaders)))
    for row in tableValues:
        print(",".join(map(str, row)))

def main():
    if len(sys.argv) != 2:
        print("ERROR : invalid args please provide arg in form of  python {} '<sql query>'".format((sys.argv[0])))
        exit(-1)
    query = sys.argv[1]
    query = query.lower()
    MiniSqlEngine(query)

if __name__ == "__main__":
    main()