@Grab('org.apache.iceberg:iceberg-api:1.6.1')
@Grab('org.apache.iceberg:iceberg-core:1.6.1')
@Grab('org.apache.iceberg:iceberg-common:1.6.1')
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import java.util.regex.Pattern

class TableInfo {

    private int index = 1

    private int nextIndex() { index++ }
    
    final Map<TableIdentifier,Schema> tables = [:]

    @Override
    String toString() {
	"tableIndentifier: ${tableIdentifier}\nschema: ${schema}"
    }

    private final static fixedTypeClosure = { s ->
	def matched = s =~ /fixed\[(\d+)\]/
	int length = matched[0][1].toInteger()
	return Types.FixedType.ofLength(length)
    }

    private final static decimalTypeClosure = { s ->
	def matched = s =~ /decimal\[(\d+)\:(\d+)\]/
	int precision = matched[0][1].toInteger()
	int scale = matched[0][2].toInteger()
	return Types.DecimalType.of(precision, scale)
    }
    
    final allTypes = [[/binary/, { ignore -> Types.BooleanType.get() }],
		      ['boolean', { ignore -> Types.BooleanType.get() }],
		      [/date/, { ignore -> Types.DateType.get() }],
		      [/decimal\[\d+:\d+\]/, decimalTypeClosure],
		      ['double', { ignore -> Types.DoubleType.get() }],
		      [/fixed\[\d+\]/, fixedTypeClosure],
		      ['float', { ignore -> Types.FloatType.get() }],
		      [/integer/, { ignore -> Types.IntegerType.get() }],
		      ['int', { ignore -> Types.IntegerType.get() }],
		      ['long', { ignore -> Types.LongType.get() }],
		      [/string/, { ignore -> Types.StringType.get() }],
		      [/timestampz/, { ignore -> Types.TimestampType.withZone() }],
		      [/timestamp/, { ignore -> Types.TimestampType.withoutZone() }],
		      [/time/, { ignore -> Types.TimeType.get() }],
		      [/uuid/, { ignore -> Types.UUIDType.get() }]]
    
    private String getTypesStr() { allTypes.collect { type -> type[0] }.join('|') }

    private Set<String> getTypes() { allTypes.collect { type -> type[0] } as Set }
    
    private Type icebergType(String str) {
	def entry = allTypes.find { list -> str ==~ list[0] }
	return entry[1].call(str)
    }

    private boolean parseOptional(String nullable) {
	if(!nullable) return true
	String toCmp = nullable.toLowerCase().replaceAll(/\s+/, '')
	if(toCmp == 'null') return true
	else if(toCmp == 'notnull') return false
	else throw new IllegalArgumentException("${nullable} not a valid optional spec")
    }

    class Column {
	int id
	String name
	Type type
	boolean optional

	Column(int id, String name, String type, String nullable) {
	    this.id = id
	    this.name = name
	    this.type = icebergType(type)
	    this.optional = parseOptional(nullable)
	}

	Types.NestedField toField() {
	    return Types.NestedField.of(id, optional, name, type)
	}

	@Override
	String toString() { "${name} ${type} ${optional}" }
    }

    private String tableNamePattern = /[a-zA-Z_][0-9a-zA-Z_\.]*/
    private String identifierPattern = /[a-zA-Z_][0-9a-zA-Z_]*/
    private String typePattern = /[a-zA-Z_][0-9a-zA-Z_]*[\[\]:\d]*/
    private Pattern getCreateRegEx() { ~/(?i)create table\s+(${tableNamePattern})\s*\(([^\(]+)\)/ }
    private Pattern getColumnRegEx() { ~/(?i)(${identifierPattern})\s+(${typePattern})\s*(.*)/ }
    private Pattern getTypeRegEx() { ~/create type\s+(${identifierPattern})\s+(list|struct|map)<([^>]+)>/ }
    
    static final String basic = """create table my_namespace.foobar(
col integer not null,
next string null,
stupid uuid
)"""

    static final String complex ="""create table foo.bar.baz (
event_id uuid not null,
user_name string not null,
user_id long not null,
api_version int null,
command string
)"""

    static final String hard = """create table blah.blam.bozo (
id long not null,
payment decimal[20:4] not null,
blob fixed[1024] null,
my_list list_of_strings not null,
my_map strs_to_longs_nulls null
)"""

    static final String createList = 'create type list_of_strings list<string>'
    static final String createStruct = 'create type my_struct struct<name string not null, age int not null, rate double null>'
    static final String createMap = 'create type strs_to_longs map<string,long>'
    static final String createMap2 = 'create type strs_to_longs_nulls map<string,long null>'
    
    private TableIdentifier toTableIdentifier(String s) {
	final List ary = s.split(/\./) as List
	final String tableName = ary[-1]
	final List elements = ary[(0..<ary.size()-1)]
	final Namespace namespace = Namespace.of(elements ? elements.join('.') : '')
	return TableIdentifier.of(namespace, tableName)
    }

    void createType(String str) {
	def matcher = str =~ typeRegEx
	assert matcher
	String typeName = matcher[0][1].toLowerCase()
	String complexType = matcher[0][2].toLowerCase()
	String typeSpec = matcher[0][3].toLowerCase().trim()

	if(types.contains(typeName))
	    throw new IllegalArgumentException("${typeName} has already been defined")

	if(complexType == 'list') {
	    allTypes.add([typeName, { ignore -> Types.ListType.ofRequired(nextIndex(), icebergType(typeSpec)) }])
	}
	else if(complexType == 'struct') {
	    def nestedFields = (typeSpec.split(',') as List).collect { sub ->
		def parts = sub.trim().split(/\s+/)
		def partName = parts[0]
		def partType = parts[1]
		def optional = (parts.length == 2) ? true : parseOptional(parts[2..<parts.size()].join(' '))
		Types.NestedField.of(nextIndex(), optional, partName, icebergType(partType))
	    }

	    final newType = Types.StructType.of(nestedFields)
	    allTypes.add([typeName, { ignore -> newType }])
	}
	else if(complexType == 'map') {
	    def (key, valueSpec) = typeSpec.split(',').collect { it.trim() }
	    def list = valueSpec.split(/\s+/)
	    def optional = (list.length == 1) ? true : parseOptional(list[1..<list.size()].join(' '))
	    def newType = (optional ?
			   Types.MapType.ofOptional(nextIndex(), nextIndex(), icebergType(key), icebergType(list[0])) :
			   Types.MapType.ofRequired(nextIndex(), nextIndex(), icebergType(key), icebergType(list[0])))
	    allTypes.add([typeName, { ignore -> newType }])
	}
    }
    
    TableInfo createTable(String stmt) {
	def matcher = stmt =~ createRegEx
	assert matcher
	
	List colDefs = matcher[0][2].trim().split(',') as List
	def fields = colDefs.collect { colDef ->
	    def colMatcher = colDef =~ columnRegEx
	    assert colMatcher
	    def col = new Column(nextIndex(), colMatcher[0][1], colMatcher[0][2], colMatcher[0][3])
	    return col.toField()
	}

	tables[toTableIdentifier(matcher[0][1])] = new Schema(fields)
	this
    }
    
    static void main(String[] args) {
	[basic, complex].each { sql ->
	    def tinfo = new TableInfo().createTable(sql)
	    println tinfo.tables
	}

	TableInfo ti = new TableInfo()
	ti.createType(createList)
	println ti.icebergType('list_of_strings')

	ti.createType(createMap)
	println ti.icebergType('strs_to_longs')

	ti.createType(createMap2)
	println ti.icebergType('strs_to_longs_nulls')

	ti.createType(createStruct)
	println ti.icebergType('my_struct')

	ti.createTable(hard)
	println ti.tables
    }
}
