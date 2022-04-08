package com.me.study.tuning.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MidCountAndWindowEnd {

    String mid;
    Long count;
    Long windowEnd;

}
