package com.six_group.dgi.dsx.bigdata.poc.quoting;

import java.math.BigDecimal;

public class MidPriceData {
    public static final int _MID_PRICE_SCALE = 5;
    public static final int _MID_VOLUME_SCALE = 0;
    private final BigDecimal _midPrice;
    private final BigDecimal _midVolume; 

    public MidPriceData(final BigDecimal price, final BigDecimal volume) {
        _midPrice = price;
        _midVolume = volume;
    } 

    public BigDecimal getMidPrice() {
        return _midPrice;
    } 

    public BigDecimal getMidVolume() {
        return _midVolume;
    } 

    public static MidPriceData zero() {
        return new MidPriceData(BigDecimal.ZERO.setScale(_MID_PRICE_SCALE), BigDecimal.ZERO.setScale(_MID_VOLUME_SCALE));
    }

}