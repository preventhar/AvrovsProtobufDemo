import com.google.protobuf.Timestamp;
import com.trivadis.avro.person.v1.Address;
import com.trivadis.avro.person.v1.Person;
import com.trivadis.avro.person.v1.TitleEnum;
import com.trivadis.protobuf.address.v1.AddressWrapper;
import com.trivadis.protobuf.lov.TitleEnumWrapper;
import com.trivadis.protobuf.person.v1.PersonWrapper;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.LocalDate;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Comparator {

    private final static String AVRO_SERIAL_FILE = "./src/main/data/person_v1.1.avro";
    private final static String PROTO_SERIAL_FILE = "./src/main/data/person_v1.1.bin";

    public static void main(String args[]) throws Exception {

        try {
            System.out.println("Avro is going to serialize");
            long startTime = System.currentTimeMillis();
            avrowrite();
            long endTime = System.currentTimeMillis();
            long timeElapsed = endTime - startTime;
            System.out.println("Avro Serialization time in ms " + timeElapsed);

            System.out.println("Avro is going to de-serialize");
            startTime = System.currentTimeMillis();
            avroRead();
            endTime = System.currentTimeMillis();
            timeElapsed = endTime - startTime;
            System.out.println("Avro de-serialization time in ms " + timeElapsed);


            System.out.println("Proto is going to serialize");
            startTime = System.currentTimeMillis();
            protoWrite();
            endTime = System.currentTimeMillis();
            timeElapsed = endTime - startTime;
            System.out.println("Proto serialization time in ms " + timeElapsed);

            System.out.println("Proto is going to de-serialize");
            startTime = System.currentTimeMillis();
            protoRead();
            endTime = System.currentTimeMillis();
            timeElapsed = endTime - startTime;
            System.out.println("Proto de-serialization time in ms " + timeElapsed);

        } catch(Exception e) {
            System.out.println("The exception is......");
            System.out.println("Trace....");
            e.printStackTrace();
            System.out.println("Message is...."+ e.getMessage());
        }
    }

    public static void avrowrite() throws Exception {

        File avroFile = new File(AVRO_SERIAL_FILE);
        if (avroFile.exists()) {
            avroFile.delete();
        }
        avroFile.createNewFile();

        List<Address> addresses = new ArrayList<Address>();
        addresses.add(Address.newBuilder()
                .setId(1)
                .setStreetAndNr("Somestreet 10")
                .setZipAndCity("9332 Somecity").build());

        Person persons = Person.newBuilder().setId(1)
                .setFirstName("Peter")
                .setMiddleName("Paul")
                .setLastName("Sample")
                .setEmailAddress("peter.sample@somecorp.com")
                .setPhoneNumber("+41 79 345 34 44")
                .setTitle(TitleEnum.Mr)
                .setBirthDate(new LocalDate("1995-11-10"))
                .setAddresses(addresses).build();

        final DatumWriter<Person> datumWriter = new SpecificDatumWriter<>(Person.class);
        final DataFileWriter<Person> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(persons.getSchema(), avroFile);
        dataFileWriter.append(persons);
        dataFileWriter.close();
    }

    public static void avroRead() throws Exception {
        final File file = new File(AVRO_SERIAL_FILE);
        final Person persons;
        final DatumReader<Person> personReader = new SpecificDatumReader<>(Person.SCHEMA$);
        final DataFileReader<Person> dataFileReader = new DataFileReader<>(file, personReader);
        persons = dataFileReader.next(new Person());
        System.out.println(persons);
    }

    public static void protoWrite() throws Exception {

        File protoFile = new File(PROTO_SERIAL_FILE);
        if (protoFile.exists()) {
            protoFile.delete();
        }
        protoFile.createNewFile();

        List<AddressWrapper.Addresss> addresses = new ArrayList<>();
        addresses.add(AddressWrapper.Addresss.newBuilder()
                .setId(1)
                .setStreetAndNr("Somestreet 10")
                .setZipAndCity("9332 Somecity").build());

        Instant time = Instant.parse("1995-11-10T00:00:00.00Z");
        Timestamp timestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond())
                .setNanos(time.getNano()).build();

        PersonWrapper.Person person = PersonWrapper.Person.newBuilder().setId(1)
                .setFirstName("Peter")
                .setMiddleName("Paul")
                .setLastName("Sample")
                .setEmailAddress("peter.sample@somecorp.com")
                .setPhoneNumber("+41 79 345 34 44")
                .setTitle(TitleEnumWrapper.Title.MR)
                .setBirthDate(timestamp)
                .addAllAddresses(addresses).build();

        FileOutputStream output = new FileOutputStream(protoFile);
        person.writeTo(output);
    }

    public static void protoRead() throws Exception {

        PersonWrapper.Person person = PersonWrapper.Person.parseFrom(new FileInputStream(PROTO_SERIAL_FILE));
        //System.out.println(person);
    }
}
