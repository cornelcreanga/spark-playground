package com.creanga.playground.spark.csv;

import me.xdrop.fuzzywuzzy.FuzzySearch;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.UrlValidator;

public class FuzzyTest {

    public static void main(String[] args) {

//        System.out.println(new UrlValidator().isValid("evz.ro"));
        System.out.println(DomainValidator.getInstance().isValid("sdf.evz.ro"));
        System.out.println(DomainValidator.getInstance().isValid("evz"));
        System.out.println(DomainValidator.getInstance().isValid("evz evz.ro"));
        System.out.println(DomainValidator.getInstance().isValid(""));

//        System.out.println(FuzzySearch.tokenSetRatio("fuzzy was a bear", "fuzzy fuzzy fuzzy bear"));
//        System.out.println(FuzzySearch.tokenSetPartialRatio("fuzzy was a bear", "fuzzy fuzzy fuzzy bear"));
        System.out.println(FuzzySearch.weightedRatio("3E Staging Inc.","3E Staging Inc. - Home Staging, Design and Renovation Services"));
        System.out.println(FuzzySearch.tokenSetRatio("3E Staging Ic.","3E Staging Inc. - Home Staging, Design and Renovation Services"));
        System.out.println(FuzzySearch.tokenSetPartialRatio("3E Staging Ic.","3E Staging Inc. - Home Staging, Design and Renovation Services"));
        System.out.println(FuzzySearch.weightedRatio("3E Staging Inc.","3E Staging Inc. - Home Staging, Design and Renovation Services"));
        System.out.println(FuzzySearch.weightedRatio("3E STAGING INC.","3E Staging Inc. - Home Staging, Design and Renovation Services"));

        System.out.println(FuzzySearch.weightedRatio("1CS - Computer Services","1ComputerServices Inc. (1CS)"));
        System.out.println(FuzzySearch.tokenSetRatio("1CS - Computer Services","1ComputerServices Inc. (1CS)"));
        System.out.println(FuzzySearch.tokenSetPartialRatio("1CS - Computer Services","1ComputerServices Inc. (1CS)"));
        System.out.println(FuzzySearch.weightedRatio("1CS - Computer Services","1ComputerServices Inc. (1CS)"));
        System.out.println(FuzzySearch.weightedRatio("1CS - Computer Services","1ComputerServices Inc. (1CS)"));
/**
 * |Rise Respite Resource Solutions Inc.                                                                                                                                                                                                                                                                   |google|
 * |3rsolutions4u.ca                      |6535 millcreek drive, l5n2m2, mississauga, on, canada, ontario                                                                      |Security Guards & Patrol Services|Community Center                                                                                        |mississauga                  |canada                                 |ontario                       |+19059979222   |RISE Respite Solutions                                                                                                                                                                                                                                                                                 |fb    |
 * |3rsolutions4u.ca                      |                                                                                                                                    |Social Services & NGOs                                                                                                                    |                             |canada                                 |ontario                       |19059979222    |Rise Respite Resource Solutions Inc.
 *  |3E Staging Inc. - Home Staging, Design and Renovation Services                                                                                                                                                                                                                                         |fb    |
 * |3estaging.com                         |13-50 West Wilmot St, Richmond Hill, ON L4B 1M5, Canada                                                                             |null                                                                                                                                      |richmond hill                |canada                                 |ontario                       |+14162947382   |3E Staging Inc.                                                                                                                                                                                                                                                                                        |google|
 * |3estaging.com                         |                                                                                                                                    |Decorators & Interior Designers                                                                                                           |                             |canada                                 |ontario                       |14162947382    |3E STAGING INC.
 */
    }
}
