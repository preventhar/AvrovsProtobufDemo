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
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Comparator {

    private final static String AVRO_SERIAL_FILE = "./src/main/data/person_v1.1.avro";
    private final static String PROTO_SERIAL_FILE = "./src/main/data/person_v1.1.bin";

    public static void main(String args[]) throws IOException {
        avrowrite();
        avroRead();
        protoWrite();
        protoRead();
    }

    public static void avrowrite() throws IOException {
        List<Person> persons = new ArrayList<Person>();
        List<Address> addresses = new ArrayList<Address>();

        addresses.add(Address.newBuilder()
                .setId(1)
                .setStreetAndNr("Somestreet 10")
                .setZipAndCity("9332 Somecity").build());

        Person person1 = Person.newBuilder().setId(1)
                .setFirstName("Peter")
                .setMiddleName("Paul")
                .setLastName("Sample")
                .setEmailAddress("peter.sample@somecorp.com")
                .setPhoneNumber("+41 79 345 34 44")
                .setTitle(TitleEnum.Mr)
                .setBirthDate(new LocalDate("1995-11-10"))
                .setAddresses(addresses).build();
        persons.add(person1);

        final DatumWriter<Person> datumWriter = new SpecificDatumWriter<>(Person.class);
        final DataFileWriter<Person> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            File avroFile =  new File(AVRO_SERIAL_FILE);
            if(avroFile.exists()) {
                avroFile.delete();
            }
            avroFile.createNewFile();

            dataFileWriter.create(persons.get(0).getSchema(), avroFile);
            persons.forEach(employee -> {
                try {
                    dataFileWriter.append(employee);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            });
        } catch (IOException e) {
            throw new RuntimeException();
        }
        finally {
            dataFileWriter.close();
        }
    }

    public static void avroRead() throws IOException {
        final File file = new File(AVRO_SERIAL_FILE);
        final List<Person> persons = new ArrayList<>();
        final DatumReader<Person> personReader = new SpecificDatumReader<>(Person.SCHEMA$);
        final DataFileReader<Person> dataFileReader = new DataFileReader<>(file, personReader);

        while (dataFileReader.hasNext()) {
            persons.add(dataFileReader.next(new Person()));
        }

        for (Person person : persons) {
            System.out.println(person);
        }
    }

    public static void protoWrite() throws IOException {
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

        File protoFile =  new File(PROTO_SERIAL_FILE);
        if(protoFile.exists()) {
            protoFile.delete();
        }
        protoFile.createNewFile();
        
        FileOutputStream output = new FileOutputStream(protoFile);
        person.writeTo(output);
    }

    public static void protoRead() throws IOException {

        PersonWrapper.Person person =
                PersonWrapper.Person.parseFrom(new FileInputStream(PROTO_SERIAL_FILE));
        System.out.println(person);
    }
}
