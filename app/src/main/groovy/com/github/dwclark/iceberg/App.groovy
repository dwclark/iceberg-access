package com.github.dwclark.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.DataFile
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory
import org.apache.iceberg.aws.s3.S3FileIOProperties
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.iceberg.io.CloseableIterable
import org.apache.iceberg.io.DataWriter
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.types.Types
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

class MyFactory implements S3FileIOAwsClientFactory {
    private static S3Client init() {
	AwsBasicCredentials identity = AwsBasicCredentials.create(System.getenv('AWS_ACCESS_KEY_ID'), System.getenv('AWS_SECRET_ACCESS_KEY'))
	StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(identity)
	return S3Client.builder()
	    .region(Region.US_EAST_1) //sdk whines if not specified
	    .endpointOverride(new URI('http://localhost:9000'))
	    .credentialsProvider(staticCredentialsProvider)
	    .forcePathStyle(true) //seems to get rid of minio complains about signing
	    .build()
    }

    @Lazy static S3Client instance = init()
    
    S3Client s3() { return instance }
    void initialize(Map<String, String> properties) {}
}

class App {

    final Map PROPS = [(CatalogProperties.CATALOG_IMPL): "org.apache.iceberg.rest.RESTCatalog",
		       (CatalogProperties.URI): "http://localhost:8181",
		       (CatalogProperties.WAREHOUSE_LOCATION): "s3://warehouse",
		       (CatalogProperties.FILE_IO_IMPL): "org.apache.iceberg.aws.s3.S3FileIO",
		       (S3FileIOProperties.CLIENT_FACTORY): MyFactory.name]
    
    final Schema schema = new Schema(Types.NestedField.optional(1, "event_id", Types.StringType.get()),
				     Types.NestedField.optional(2, "username", Types.StringType.get()),
				     Types.NestedField.optional(3, "userid", Types.IntegerType.get()),
				     Types.NestedField.optional(4, "api_version", Types.StringType.get()),
				     Types.NestedField.optional(5, "command", Types.StringType.get()))
    
    final PartitionSpec partitionSpec = PartitionSpec.unpartitioned()
    
    final GenericRecord example = GenericRecord.create(schema)
    
    final Namespace webapp = Namespace.of("webapp")
    
    final TableIdentifier name = TableIdentifier.of(webapp, "user_events")
    
    private RESTCatalog initCatalog() {
	RESTCatalog catalog = new RESTCatalog(conf: new Configuration())
	catalog.initialize("demo", PROPS)
	return catalog
    }

    private void initNamespace() {
	try {
	    catalog.listNamespaces(webapp)
	}
	catch(NoSuchNamespaceException nsne) {
	    catalog.createNamespace(webapp, [:])
	}
    }
    
    private Table initTable() {
	initNamespace()
	if(!catalog.tableExists(name))
	    return catalog.createTable(name, schema, partitionSpec)
	else
	    return catalog.loadTable(name)
    }

    @Lazy RESTCatalog catalog = initCatalog()
    @Lazy Table table = initTable()

    private GenericRecord newRecord(Map args) {
	return example.copy([event_id: UUID.randomUUID().toString()] + args)
    }
    
    private List<GenericRecord> newRecords() {
	return [newRecord(username: "Bruce", userid: 1, api_version: "1.0", command: "grapple"),
		newRecord(username: "Wayne", userid: 1, api_version: "1.0", command: "glide"),
		newRecord(username: "Clark", userid: 1, api_version: "2.0", command: "fly"),
		newRecord(username: "Kent", userid: 1, api_version: "1.0", command: "land")]
    }

    private void save(List<GenericRecord> records) {
	OutputFile file = table.io().newOutputFile("${table.location()}/${UUID.randomUUID()}")
	DataFile dataFile = Parquet.writeData(file)
	    .schema(schema)
	    .createWriterFunc(GenericParquetWriter::buildWriter)
	    .overwrite()
	    .withSpec(partitionSpec)
	    .build()
	    .withCloseable { writer ->
		records.each { r -> writer.write(r) }
		return writer
	    }.toDataFile()
	
	table.newAppend().appendFile(dataFile).commit()
    }

    private void showTable() {
	IcebergGenerics.read(table).build().eachWithIndex { record, index -> println "${index}: ${record}" }
    }
    
    static void main(String[] args) {
	App app = new App()
	app.save(app.newRecords())
	app.showTable()
    }
}
