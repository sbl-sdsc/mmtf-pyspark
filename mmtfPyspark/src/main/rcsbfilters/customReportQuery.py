'''
customReportQuery.py

This filter runs an SQL query on specified PDB metadata and annotation fields retrived using
RCSB PDB RESTful web services. The fields are then queried and the resulting PDB IDs are
used to filter the data. The input to the filter consists of an SQL WHERE clause, and list
data columns availible from RCSB PDB web services.

<p>See <a href="http://www.rcsb.org/pdb/results/reportField.do"> list of supported
field names.</a>

<p>See <a href="https://www.w3schools.com/sql/sql_where.asp"> for examples of
SQL WHERE clauses.</a>

<p>Example: find PDB entries with Enzyme classification number 2.7.11.1
and source organism Homo sapiens:

<pre><code>
    JavaPairRDD<String, StructureDataInterface> pdb = ...
    String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "ecNo","source"));
</code></pre>

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Done"
'''

# TODO need customReportService from /main/dataset/
