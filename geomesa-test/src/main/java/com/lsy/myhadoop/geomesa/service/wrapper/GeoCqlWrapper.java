package com.lsy.myhadoop.geomesa.service.wrapper;

import com.lsy.myhadoop.geomesa.service.constant.ExceptionEnum;
import com.lsy.myhadoop.geomesa.service.constant.WrapperConstant;
import com.lsy.myhadoop.geomesa.service.entity.GeomesaContext;
import com.lsy.myhadoop.geomesa.service.entity.ParamOperationInfo;
import com.lsy.myhadoop.geomesa.service.utils.BusiVerify;
import com.lsy.myhadoop.geomesa.service.wrapper.builder.QueryBuilder;
import com.lsy.myhadoop.geomesa.service.wrapper.enums.OperatorEnum;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GeoCqlWrapper implements Wrapper<GeomesaContext> {

    @Override
    public void wrap(GeomesaContext geomesaContext) {
        List<String> querys = new ArrayList<>();

        for (ParamOperationInfo paramOperationInfo: geomesaContext.getParamList()) {
            OperatorEnum operator = OperatorEnum.valueOf(paramOperationInfo.getOperationCode());
            BusiVerify.verify(operator.check(paramOperationInfo, geomesaContext), ExceptionEnum.PARAM_INVALID.getCode(), String.format("%s校验错误", operator.name()));

            QueryBuilder queryBuilder = operator.operate(paramOperationInfo);

            querys.add(queryBuilder.asCqlFilterString());
        }

        geomesaContext.setCql(String.join(WrapperConstant.AND, querys));
    }
}
