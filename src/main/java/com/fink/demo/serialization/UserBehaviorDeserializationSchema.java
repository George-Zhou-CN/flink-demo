package com.fink.demo.serialization;

import com.alibaba.fastjson.JSON;
import com.fink.demo.model.UserBehavior;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * Created by Jiazhi on 2020/6/22.
 */
public class UserBehaviorDeserializationSchema implements DeserializationSchema<UserBehavior> {

    @Override
    public UserBehavior deserialize(byte[] message) throws IOException {
        String json = new String(message, "UTF-8");
        UserBehavior userBehavior = JSON.parseObject(json, UserBehavior.class);
        return userBehavior;
    }

    @Override
    public boolean isEndOfStream(UserBehavior nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
