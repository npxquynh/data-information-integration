# Create table in HBase
result = ""

with open("column_families.txt") as input_file:
    for line in input_file:
        result = result + "," + line.strip()


# Map table in Hive for HBase
query_1 = "key STRING" # CREATE EXTERNAL TABLE foo(k STRING, ab1 INT, ab2 INT)
query_2 = ":key" # ":key,f1:c1,f1:c3"
with open("column_families_datatype.txt") as input_file:
    for line in input_file:
        fields = line.strip().split("\t")
        if fields[1] == "INTEGER":
            fields[1] = "INT"

        query_1 = query_1 + ", %s %s" % (fields[0], fields[1])
        query_2 = query_2 + ", %s" % (fields[0])



