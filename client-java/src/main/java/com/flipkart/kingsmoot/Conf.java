package com.flipkart.kingsmoot;

import lombok.Value;

@Value
public class Conf {
    private final String[] servers;
    private final Backend backend;
}
