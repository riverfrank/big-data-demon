package com.river.tag.dto;


import lombok.Data;

import java.util.List;

@Data
public class ReviewVo {
    private List<ReviewPic> reviewPics;
    private List<ExtInfo> extInfoList;
    private List<Integer> reviewIndexes;
    private List<Score> scoreList;
}
