package com.me.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zs
 * @date 2022/3/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo{
  Integer id;
  Long userId;
  Double totalAmount;
  Long createTime;
}
