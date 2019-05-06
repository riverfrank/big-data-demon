package com.river.tag.dto;

import lombok.Data;

import java.util.List;

/**
 * @author riverfan
 */
@Data
public class ExtInfo {
    private String title;
    private List<String> values;
    private String desc;
    private Integer defineType;
}
