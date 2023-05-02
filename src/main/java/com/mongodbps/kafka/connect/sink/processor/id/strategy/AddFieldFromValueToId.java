package com.mongodbps.kafka.connect.sink.processor.id.strategy;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.DocumentIdAdder;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddFieldFromValueToId extends PostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddFieldFromValueToId.class);
    private final boolean overwriteExistingIdValues;

    private final String TARGET_ID_FIELD_NAME = "myId";

    public AddFieldFromValueToId(MongoSinkTopicConfig config) {
        super(config);
        this.overwriteExistingIdValues = config.getBoolean("document.id.strategy.overwrite.existing");
    }

    @Override
    public void process(SinkDocument sinkDocument, SinkRecord sinkRecord) {
        sinkDocument.getValueDoc().ifPresent((vd) -> {
            if (this.shouldAppend(vd)) {
                vd.append("_id", vd.get(TARGET_ID_FIELD_NAME));
            } else if (vd.containsKey("_id")) {
                LOGGER.warn("Cannot overwrite the existing '{}' value. '{}' is set to false and the document.", "_id", "document.id.strategy.overwrite.existing");
            }

        });
    }

    private boolean shouldAppend(BsonDocument doc) {
        return !doc.containsKey("_id") || this.overwriteExistingIdValues;
    }
}
