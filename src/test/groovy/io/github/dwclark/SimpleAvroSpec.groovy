package io.github.dwclark;

import spock.lang.*;
import groovy.json.*;
import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.apache.avro.file.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.rules.TemporaryFolder;
import org.junit.Rule;
import org.apache.parquet.avro.*;

class SimpleAvroSpec extends Specification {

    @Rule TemporaryFolder folder;
    
    final Map SP_SPEC = [type: 'record', name: 'StringPair', doc: 'A pair of strings',
                         fields: [[name: 'left', type: 'string'],
                                  [name: 'right', type: 'string'] ] ];
    final String STR_SP_SPEC = new JsonBuilder(SP_SPEC).toString();
    
    def 'test simple generic functionality'() {
        setup:

        println(STR_SP_SPEC);
        Schema schema = new Schema.Parser().parse(STR_SP_SPEC);
        def datum = new GenericData.Record(schema);
        datum.put('left', 'L');
        datum.put('right', 'R');

        def os = new ByteArrayOutputStream();
        def writer = new GenericDatumWriter(schema);
        def enc = EncoderFactory.get().binaryEncoder(os, null);
        writer.write(datum, enc);
        enc.flush();
        os.close();

        def reader = new GenericDatumReader<>(schema);
        def dec = DecoderFactory.get().binaryDecoder(os.toByteArray(), null);
        def result = reader.read(null, dec);

        expect:

        result.get('left').toString() == 'L';
        result.get('right').toString() == 'R';
    }

    def 'test simple specific functionality'() {
        setup:

        StringPair datum = new StringPair();
        datum.left = 'L';
        datum.right = 'R';
        
        def os = new ByteArrayOutputStream();
        def writer = new SpecificDatumWriter(StringPair);
        def enc = EncoderFactory.get().binaryEncoder(os, null);
        writer.write(datum, enc);
        enc.flush();
        os.close();

        def reader = new SpecificDatumReader(StringPair);
        def dec = DecoderFactory.get().binaryDecoder(os.toByteArray(), null);
        def sp = reader.read(null, dec);

        expect:
        
        sp.left == 'L';
        sp.right == 'R';
    }

    def 'test writing and reading file'() {
        setup:
            
        def os = new ByteArrayOutputStream();
        def writer = new SpecificDatumWriter(NumToString);
        def fw = new DataFileWriter(writer);
        fw.create(NumToString.classSchema, os);
        NumToString tmp = new NumToString();
        (0..10).each { num ->
            tmp.num = num;
            tmp.str = num.toString();
            fw.append(tmp);
        }
        fw.close();

        def is = new SeekableByteArrayInput(os.toByteArray());
        def reader = new SpecificDatumReader(NumToString);
        def fr = new DataFileReader(is, reader);

        expect:

        (0..10).every { num ->
            def val = fr.next();
            val.num == num;
            val.str = num.toString();
        };

        !fr.hasNext();
    }

    def 'test writing and reading ordered file'() {
        setup:
            
        def os = new ByteArrayOutputStream();
        def writer = new SpecificDatumWriter(OrderedNumToString);
        def fw = new DataFileWriter(writer);
        fw.create(OrderedNumToString.classSchema, os);
        def tmp = new OrderedNumToString();
        //write it out of order
        [ 1, 3, 5, 7, 9, 0, 2, 4, 6, 8, 10 ].each { num ->
            tmp.num = num;
            tmp.str = num.toString();
            fw.append(tmp);
        }
        fw.close();

        def is = new SeekableByteArrayInput(os.toByteArray());
        def reader = new SpecificDatumReader(OrderedNumToString);
        def fr = new DataFileReader(is, reader);

        expect:

        (0..10).every { num ->
            def val = fr.next();
            val.num == num;
            val.str = num.toString();
        };

        !fr.hasNext();
    }

    def 'writing and reading avro/parquet'() {
        setup:

        def uri = new File(folder.root, "foo.bin").toURI();
        def path = new Path(uri);
        def writer = AvroParquetWriter.builder(path).withSchema(NumToString.classSchema).build();
        def list = (0..10).collect { num ->
            def tmp = new NumToString();
            tmp.num = num;
            tmp.str = num.toString();
            writer.write(tmp);
            return tmp;
        }

        writer.close();

        def reader = AvroParquetReader.builder(path).build();
        def o;
        def readBack = [];
        while((o = reader.read()) != null) {
            readBack << o;
        }
        
        expect:

        list == readBack;
        readBack.every { obj -> obj instanceof NumToString }
    }

    static String STR_PROJECTION = '''
{
    "type":"record",
    "name":"NumToString",
    "namespace": "io.github.dwclark",
    "doc":"Translation from number to string representation",
    "fields":[
        {"name":"num","type":"int"}
    ]
}

'''
    
    def 'writing and reading avro/parquet with projection'() {
        setup:

        def uri = new File(folder.root, "foo.bin").toURI();
        def path = new Path(uri);
        def writer = AvroParquetWriter.builder(path).withSchema(NumToString.classSchema).build();
        def list = (0..10).collect { num ->
            def tmp = new NumToString();
            tmp.num = num;
            tmp.str = num.toString();
            writer.write(tmp);
            return tmp;
        }

        writer.close();

        Schema schema = new Schema.Parser().parse(STR_PROJECTION);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, schema);
        def reader = AvroParquetReader.builder(path).withConf(conf).build();
        def o;
        def readBack = [];
        while((o = reader.read()) != null) {
            readBack << o;
        }
        def nextNum = 0;
        
        expect:

        readBack.every { obj -> obj.str == null && obj.num == nextNum++; }
    }
}
