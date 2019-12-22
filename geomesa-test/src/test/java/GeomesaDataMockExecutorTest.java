import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;
import com.lsy.myhadoop.geomesa.service.executor.impl.AbstractGeomesaExecutor;
import lombok.extern.slf4j.Slf4j;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.factory.Hints;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.identity.FeatureIdImpl;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.*;

@Slf4j
public class GeomesaDataMockExecutorTest extends AbstractGeomesaExecutor<GeomesaContext, Integer> {

    private int count = 100000;
    private String catalog = "catlog_vehicle";
    private String type = "ftn_taxi";

    @Override
    public Integer queryResult(DataStore dataStore, GeomesaContext geomesaContext) throws IOException {

        catalog = geomesaContext.getCatalog();
        type = geomesaContext.getFeature();

        SimpleFeatureType sft = dataStore.getSchema(geomesaContext.getFeature());

        writeFeatures(dataStore, sft);

        return count;
    }

    public void writeFeatures(DataStore dataStore, SimpleFeatureType sft) throws IOException {

        List<SimpleFeature> features = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            SimpleFeature feature = "catlog_vehicle".equals(catalog) ? mockVechicleFeature(sft) : mockDefaultFeature(sft);
            features.add(feature);
        }
        writeFeatures(dataStore, sft, features);
    }

    private void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        if (features.size() <= 0)
            return ;

        FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT);

        for (SimpleFeature feature : features) {
            SimpleFeature toWrite = writer.next();

            toWrite.setAttributes(feature.getAttributes());

            ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
            toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

            toWrite.getUserData().putAll(feature.getUserData());

            writer.write();
        }

        writer.close();
    }

    private SimpleFeature mockVechicleFeature(SimpleFeatureType sft) {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        String id = "鲁A" + String.valueOf(randomInt(1000, 9999));
        String type = "01";
        long ts = 1000L * randomInt(1546272000, 1548950400);
        double lng = 1.0 * randomInt(116900, 117900) / 1000;
        double lat = 1.0 * randomInt(366000, 375000) / 10000;
        double speed = 1.0 * randomInt(0, 1000) / 100;
        double direction = 1.0 * randomInt(-1800, 3600) / 10;
        String geom = "POINT (" + lng + " " + lat + ")";
        String key = id + "_" + type + "-" + ts;

        Map<String, Object> rec = new HashMap<>();
        rec.put("_id", id);
        rec.put("_type", type);
        rec.put("_ts", new Date(ts));
        rec.put("_lng", lng);
        rec.put("_lat", lat);
        rec.put("_linkid", String.valueOf(randomInt(10000, 100000)));
        rec.put("_driverid", String.valueOf(randomInt(1000000, 10000000)));
        rec.put("_countyid", String.valueOf(randomInt(10000, 100000)));
        rec.put("_speed", speed);
        rec.put("_direction", direction < 0 ? -1 : direction);
        rec.put("_elevation", 0.1 * randomInt(20, 100));
        rec.put("_status", 1);

        for (String name : rec.keySet()) {
            builder.set(name, rec.get(name));
        }
        builder.set("_value", (rec).toString());
        builder.set("geom", geom);
        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

        return builder.buildFeature(key);
    }

    private SimpleFeature mockDefaultFeature(SimpleFeatureType sft) {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
        builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);

        String key = String.valueOf(randomInt(100000, 1000000));
        return builder.buildFeature(key);
    }

    private int randomInt(int min, int max) {
        Random random = new Random();
        return random.nextInt(max) % (max - min + 1) + min;
    }


}
