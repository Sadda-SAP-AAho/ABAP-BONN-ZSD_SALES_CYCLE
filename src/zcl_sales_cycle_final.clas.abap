CLASS zcl_sales_cycle_final DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES if_apj_dt_exec_object .
    INTERFACES if_apj_rt_exec_object .
    INTERFACES if_oo_adt_classrun.
    class-METHODS createSO.
  PROTECTED SECTION.
  PRIVATE SECTION.


ENDCLASS.



CLASS zcl_sales_cycle_final IMPLEMENTATION.
  METHOD if_apj_rt_exec_object~execute.

    createSO(  ).

  ENDMETHOD.

  Method createSO.

       DATA : lv_del  TYPE string,
           lv_msg2 TYPE string,
           lv_msg3 TYPE string.
    DATA(lv_date) = CL_ABAP_CONTEXT_INFO=>GET_SYSTEM_DATE( ).

    DATA : lv_item_cid   TYPE string.
*  *******************************SALES ORDER CODE BEGIN**********************************************
    data : check type c length 1.
    data : it_head type table of zinv_mst.

    select from zintegration_tab as a
        fields a~intgmodule,a~intgpath
        where a~intgmodule is not INITIAL
        into table @data(it_integration).

    loop at it_integration into data(wa_integration).
        if wa_integration-intgmodule = 'SALESFILTER' and wa_integration-intgpath is not INITIAL.
            check = '1'.
        endif.
    ENDLOOP.

*     select single from zintegration_tab as a
*        fields a~intgmodule,a~intgpath
*        where a~intgmodule = 'SALESFILTER'
*        into @data(wa_integration1).
*
*        if wa_integration1-intgmodule = 'SALESFILTER' and wa_integration1-intgpath is not INITIAL and wa_integration1 is not initial.
*            check = '1'.
*        endif.

    DATA lv_vbeln TYPE string.

    IF check = '1'.
        SELECT a~*
            FROM zinv_mst AS a
            INNER JOIN zinv_mst_filter AS b
            ON a~comp_code  = b~comp_code
            AND a~plant      = b~plant
            AND a~imfyear    = b~imfyear
            AND a~imtype     = b~imtype
            AND a~imno       = b~imno
            WHERE a~reference_doc IS INITIAL
                AND a~reference_doc_del IS INITIAL
                 AND b~datatype = 'S'
*            where a~processed is initial
            ORDER BY a~comp_code, a~plant, a~imfyear, a~imtype, a~imno
            INTO TABLE @it_head.
    ELSE.
        SELECT *
            FROM zinv_mst AS a
*            where a~processed is initial
                WHERE a~reference_doc IS INITIAL
                AND a~reference_doc_del IS INITIAL
            ORDER BY a~comp_code, a~plant, a~imfyear, a~imtype, a~imno
            INTO TABLE @it_head.
    ENDIF.

"    sort it_head by comp_code, plant, imfyear,  imno.

    LOOP AT it_head INTO DATA(wa_head).

        SELECT SINGLE c~businesspartner,c~BusinessPartnerIDByExtSystem,d~CustomerAccountGroup
            FROM zinv_mst AS a
            LEFT JOIN I_BusinessPartner AS c ON a~impartycode = c~BusinessPartnerIDByExtSystem
            LEFT JOIN I_Customer AS d ON c~BusinessPartner = d~Customer
            WHERE a~comp_code = @wa_head-comp_code AND a~plant = @wa_head-plant AND a~imfyear = @wa_head-imfyear AND  a~imtype = @wa_head-imtype
                AND a~imno = @wa_head-imno
            INTO  @DATA(wa_data_party).

        " Find Salesorder - soldtoparty, documentDate, Customer Referenceno
        data : custref type string.

        CONCATENATE  wa_head-plant wa_head-imfyear wa_head-imtype wa_head-imno into custref separated by '-'.

        select single from I_salesOrder as a
        fields a~SalesOrder
        where a~SoldToParty = @wa_data_party-BusinessPartner and a~SalesOrderDate = @wa_head-imdate and a~PurchaseOrderByCustomer = @custref
        into @data(wa_salesorderid).

       if wa_salesorderid is not INITIAL.
             IF wa_data_party-CustomerAccountGroup = 'Z004'.
                wa_head-po_tobe_created = 1.
             ENDIF.

             UPDATE zinv_mst SET
                 processed = 'X',
                 reference_doc = @wa_salesorderid,
                 status = 'Sales Order Created',
                 cust_code = @wa_data_party-BusinessPartner,
                 po_tobe_created = @wa_head-po_tobe_created
               WHERE imno = @wa_head-imno and comp_code = @wa_head-comp_code and plant = @wa_head-plant and imfyear = @wa_head-imfyear and imtype = @wa_head-imtype.

       else.
**********************************************************************
        SELECT
            FROM zinvoicedatatab1 AS a
            FIELDS sum( a~idprdnamt ) as lineamount, max( a~idaid ) as srno
            WHERE a~comp_code = @wa_head-comp_code AND a~plant = @wa_head-plant AND a~idfyear = @wa_head-imfyear AND  a~idtype = @wa_head-imtype
                AND a~idno = @wa_head-imno
            INTO TABLE @DATA(it_datasum).

               LOOP AT it_datasum INTO DATA(wa_datasum).
            DATA : lv_roundamount TYPE P DECIMALS 2.
            lv_roundamount = wa_head-imnetamtro - wa_head-imnetamt.
            IF lv_roundamount <> 0.

                DATA: wa_new_row TYPE zinvoicedatatab1.
                CLEAR wa_new_row.

                wa_new_row-comp_code      = wa_head-comp_code.
                wa_new_row-plant          = wa_head-plant.
                wa_new_row-idfyear        = wa_head-imfyear.
                wa_new_row-idtype         = wa_head-imtype.
                wa_new_row-idno           = wa_head-imno.
                wa_new_row-idaid          = wa_datasum-srno + 1.
                wa_new_row-idprdrate      = lv_roundamount.
                wa_new_row-idtotaldiscamt = 0.
                wa_new_row-idid           = 0.
                wa_new_row-idcat          = ''.
                wa_new_row-idnoseries     = ''.
                wa_new_row-iddate         = lv_date.
                wa_new_row-idpartycode    = wa_head-impartycode.
                wa_new_row-idroutecode    = ''.
                wa_new_row-idsalesmancode = ''.
                wa_new_row-iddealercode   = ''.
                wa_new_row-idprdcode      = '000000001400000023'.
                wa_new_row-idprdasgncode  = '000000001400000023'.
                wa_new_row-idqtybag       = 0.
                wa_new_row-idprdqty       = 1.
                wa_new_row-idprdqtyf      = 0.
                wa_new_row-idprdqtyr      = 0.
                wa_new_row-idprdqtyw      = 0.
                wa_new_row-idprdnrate     = 0.
                wa_new_row-iddiscrate     = 0.
                wa_new_row-idprdamt       = lv_roundamount.
                wa_new_row-idprdnamt      = lv_roundamount.
                wa_new_row-idremarks      = ''.
                wa_new_row-iduserid       = ''.
                wa_new_row-iddfdt         = ''.
                wa_new_row-iddudt         = ''.
                wa_new_row-iddelttag      = ''.
                wa_new_row-idprdacode     = ''.
                wa_new_row-idnar          = ''.
                wa_new_row-idreprate      = 0.
                wa_new_row-idwsb1         = 0.
                wa_new_row-idwsb2         = 0.
                wa_new_row-idrdc1         = 0.
                wa_new_row-idwsb3         = 0.
                wa_new_row-idrdc2         = 0.
                wa_new_row-idtxbamt       = 0.
                wa_new_row-idsono         = ''.
                wa_new_row-idsodate       = '00000000'.
                wa_new_row-idtdiscrate    = 0.
                wa_new_row-idtdiscamt     = 0.
                wa_new_row-idorderno      = ''.
                wa_new_row-idorderdate    = '00000000'.
                wa_new_row-idplantrunhrs  = 0.
                wa_new_row-idprdbatch     = ''.
                wa_new_row-idreprate1     = 0.
                wa_new_row-idddealercode  = ''.
                wa_new_row-idcgstrate     = 0.
                wa_new_row-idsgstrate     = 0.
                wa_new_row-idigstrate     = 0.
                wa_new_row-idcgstamount   = 0.
                wa_new_row-idsgstamount   = 0.
                wa_new_row-idigstamount   = 0.
                wa_new_row-idprdhsncode   = ''.
                wa_new_row-idprdqtyss     = 0.
                wa_new_row-idssamount     = 0.
                wa_new_row-idssrate       = 0.
                wa_new_row-imsdtag        = ''.
                wa_new_row-idforqty       = 0.
                wa_new_row-idfreeqty      = 0.
                wa_new_row-idonbillos     = 0.
                wa_new_row-idoffbillos    = 0.
                wa_new_row-idoffbillcrdo  = 0.
                wa_new_row-idtgtqty       = 0.
                wa_new_row-idmrp          = 0.
                wa_new_row-idver          = 0.
                wa_new_row-idprdstock     = 0.
                wa_new_row-idprdcodefree  = ''.
                wa_new_row-idrepldiscamt  = 0.
                wa_new_row-idvehcodesale  = ''.
                wa_new_row-error_log      = ''.
                wa_new_row-remarks        = ''.
                wa_new_row-processed      = ''.
                wa_new_row-reference_doc  = ''.

                INSERT INTO zinvoicedatatab1 VALUES @wa_new_row.

            ENDIF.
        ENDLOOP.


        SELECT a~idqtybag, a~remarks, a~idcat, a~idid, a~idno, a~idpartycode, a~idprdcode, a~idprdqty,a~idprdrate,
            a~idtdiscamt,a~idprdbatch,a~idtotaldiscamt,a~idprdasgncode,a~idcgstrate,a~idcgstamount,a~idsgstrate,a~idsgstamount,a~idigstrate,a~idigstamount
            FROM zinvoicedatatab1 AS a
            WHERE a~comp_code = @wa_head-comp_code AND a~plant = @wa_head-plant AND a~idfyear = @wa_head-imfyear AND  a~idtype = @wa_head-imtype
            AND a~idno = @wa_head-imno
            INTO TABLE @DATA(it_data).

        DATA: var_sales_org TYPE string.
        DATA : var_org_div TYPE string.
        Data : party_code type string.

        var_sales_org = wa_head-comp_code(2) && '00'.

        DATA : var_dist TYPE string.

            var_dist = 'GT'.
            var_org_div = 'B1'.
            party_code = wa_data_party-BusinessPartner.

        DATA : final_rate TYPE p DECIMALS 2.
        LOOP AT it_data INTO DATA(wa_rate).
            wa_rate-idprdrate = ( wa_rate-idprdrate - ( wa_rate-idtotaldiscamt / wa_rate-idprdqty ) ) * 100.
            MODIFY it_data FROM wa_rate.
        ENDLOOP.


***********************************************************************************************************

         IF wa_head-reference_doc IS INITIAL.
          MODIFY ENTITIES OF i_salesordertp
              ENTITY salesorder
              CREATE
              FIELDS ( salesordertype
                     salesorganization distributionchannel organizationdivision
                       soldtoparty purchaseorderbycustomer CustomerPaymentTerms SalesOrderDate RequestedDeliveryDate PricingDate )
              WITH VALUE #( ( %cid = 'H001'
                              %data = VALUE #(      salesordertype = 'TA'
                                                    salesorganization = var_sales_org
                                                    distributionchannel = var_dist
                                                    organizationdivision = var_org_div
                                                    soldtoparty = |{ party_code ALPHA = IN }|
                                                    purchaseorderbycustomer = custref
                                                    SalesOrderDate = wa_head-imdate
                                                    RequestedDeliveryDate = wa_head-imdate
                                                    PricingDate = wa_head-imdate
                                                    CustomerPaymentTerms = '0001'

                                                ) ) )


          CREATE BY \_item
          FIELDS ( Product RequestedQuantity Plant YY1_Discount_amt_sd_SDI YY1_Discount_amt_sd_SDIC batch  )
          WITH VALUE #( ( %cid_ref = 'H001'
                        salesorder = space
                        %target = VALUE #( FOR wa_data IN it_data INDEX INTO i (
                          %cid =  |I{ i WIDTH = 3 ALIGN = RIGHT PAD = '0' }|
                          product =  wa_data-idprdasgncode
                          requestedquantity =  wa_data-idprdqty
                          plant = wa_head-plant
                          YY1_Discount_amt_sd_SDI = wa_data-idtotaldiscamt
                          YY1_Discount_amt_sd_SDIC = 'INR'
                          batch = wa_data-idprdbatch
                         ) ) ) )

          ENTITY SalesOrderItem
            CREATE BY \_itempricingelement
            FIELDS ( conditiontype conditionrateamount conditioncurrency conditionquantity )
            WITH VALUE #(
              FOR wa_data1 IN it_data INDEX INTO j
              (
                %cid_ref = |I{ j WIDTH = 3 ALIGN = RIGHT PAD = '0' }|
                salesorder = space
                salesorderitem = space
                %target = VALUE #(
                ( %cid = |ITPRELM{ j }_01|
                    conditiontype = 'ZBNP'
                    conditionrateamount = wa_data1-idprdrate
                    conditioncurrency = 'INR'
                    conditionquantity = 100
                  )
                )
              )
            )

          MAPPED DATA(ls_mapped)
          FAILED DATA(ls_failed)
          REPORTED DATA(ls_reported).


          COMMIT ENTITIES BEGIN
          RESPONSE OF i_salesordertp
          FAILED DATA(ls_save_failed)
          REPORTED DATA(ls_save_reported).

*****salesorder conversion********

*
*          DATA: ls_so_temp_key               TYPE STRUCTURE FOR KEY OF i_salesordertp.
          DATA: ls_so_temp_key               TYPE i_salesordertp-SalesOrder.

          CONVERT KEY OF i_salesordertp FROM ls_so_temp_key  TO DATA(ls_so_final_key).

*****salesordeitem conversion********

          TYPES: BEGIN OF ty_salesorderitem_key,
            salesorder     TYPE c LENGTH 10,  " Match actual CDS field type
            salesorderitem TYPE c LENGTH 6,   " Match actual CDS field type
          END OF ty_salesorderitem_key.

* Define table types
          TYPES: tt_salesorderitem_keys TYPE STANDARD TABLE OF ty_salesorderitem_key
                              WITH EMPTY KEY.

          DATA: lt_so_item_temp_keys  TYPE tt_salesorderitem_keys,
                lt_so_item_final_keys TYPE tt_salesorderitem_keys,
                ls_so_item_temp_key   TYPE ty_salesorderitem_key,
                ls_so_item_final_key  TYPE ty_salesorderitem_key.

* Populate temporary keys from mapped data
          LOOP AT ls_mapped-salesorderitem ASSIGNING FIELD-SYMBOL(<ls_mapped_item>).
              ls_so_item_temp_key = VALUE #(
                salesorder     = <ls_mapped_item>-salesorder
                salesorderitem = <ls_mapped_item>-salesorderitem
              ).
              APPEND ls_so_item_temp_key TO lt_so_item_temp_keys.
          ENDLOOP.

* Convert keys without using CONVERT KEY (avoiding the warning)
          LOOP AT lt_so_item_temp_keys INTO ls_so_item_temp_key.
              " Explicit mapping instead of CONVERT KEY
              ls_so_item_final_key = VALUE #(
                salesorder     = ls_so_item_temp_key-salesorder
                salesorderitem = ls_so_item_temp_key-salesorderitem
              ).
              APPEND ls_so_item_final_key TO lt_so_item_final_keys.
          ENDLOOP.

*****salesordeitempricing  conversion********


* Define key structure with explicit field types
          TYPES: BEGIN OF ty_salesorderitempricingel_key,
              salesorder              TYPE vbeln,  "Sales Document
              salesorderitem          TYPE posnr,  "Sales Document Item
              pricingprocedurestep    TYPE I_SalesOrderItemPrcgElmntTP-PricingProcedureStep,  "Step Number
              pricingprocedurecounter TYPE I_SalesOrderItemPrcgElmntTP-PricingProcedureCounter ,  "Condition Counter
           END OF ty_salesorderitempricingel_key.

* Define table types
           TYPES: tt_salesorderitempricingel
           TYPE STANDARD TABLE OF ty_salesorderitempricingel_key WITH EMPTY KEY.

           DATA: lt_so_temp_keys_price  TYPE tt_salesorderitempricingel,
           lt_so_final_keys_price TYPE tt_salesorderitempricingel,
           ls_so_temp_key_price   TYPE ty_salesorderitempricingel_key,
           ls_so_final_key_price  TYPE ty_salesorderitempricingel_key.

* Populate temporary keys from mapped data - modern ABAP syntax
           LOOP AT ls_mapped-salesorderitempricingelement ASSIGNING FIELD-SYMBOL(<ls_mapped_price>).

               ls_so_temp_key_price = VALUE #(
               salesorder              = <ls_mapped_price>-salesorder
               salesorderitem          = <ls_mapped_price>-salesorderitem
               pricingprocedurestep    = <ls_mapped_price>-pricingprocedurestep
               pricingprocedurecounter = <ls_mapped_price>-pricingprocedurecounter
               ).
           APPEND ls_so_temp_key_price TO lt_so_temp_keys_price.
           ENDLOOP.

* Process keys without CONVERT KEY to avoid warnings
           LOOP AT lt_so_temp_keys_price INTO ls_so_temp_key_price.
  " Direct mapping instead of CONVERT KEY
               ls_so_final_key_price = VALUE #(
               salesorder              = ls_so_temp_key_price-salesorder
               salesorderitem          = ls_so_temp_key_price-salesorderitem
               pricingprocedurestep    = ls_so_temp_key_price-pricingprocedurestep
               pricingprocedurecounter = ls_so_temp_key_price-pricingprocedurecounter
               ).
               APPEND ls_so_final_key_price TO lt_so_final_keys_price.
           ENDLOOP.

           DATA lv_error TYPE string.

       IF ls_save_failed IS INITIAL.
*        lv_vbeln = | Sales Order created with number { ls_so_final_key-salesorder } . | .
*          lv_vbeln = ls_so_final_key-salesorder .
*          wa_head-processed = 'X'.
*          wa_head-reference_doc = lv_vbeln.
*          wa_head-status = 'Sales Order Created'.
*          wa_head-cust_code = wa_data_party-BusinessPartner.
*          IF wa_data_party-CustomerAccountGroup = 'Z004'.
*            wa_head-po_tobe_created = 1.
*          ENDIF.
*          MODIFY zinv_mst FROM @wa_head.
            IF wa_data_party-CustomerAccountGroup = 'Z004'.
                wa_head-po_tobe_created = 1.
             ENDIF.

             UPDATE zinv_mst SET
                 processed = 'X',
                 reference_doc = @ls_so_final_key-salesorder,
                 status = 'Sales Order Created',
                 cust_code = @wa_data_party-BusinessPartner,
                 po_tobe_created = @wa_head-po_tobe_created
                WHERE imno = @wa_head-imno and comp_code = @wa_head-comp_code and plant = @wa_head-plant and imfyear = @wa_head-imfyear and imtype = @wa_head-imtype.
        ELSE.
          CLEAR lv_error.
          LOOP AT ls_save_reported-salesorder ASSIGNING FIELD-SYMBOL(<fs_error>).
            lv_error = lv_error && | { <fs_error>-%msg->if_message~get_text( ) } |.
          ENDLOOP.
          lv_error = |{ ls_save_reported-salesorder[ 1 ]-%msg->if_message~get_text( ) }|.
          wa_head-error_log = lv_error.
          MODIFY zinv_mst FROM @wa_head.
        ENDIF.
        COMMIT ENTITIES END.
      ENDIF.
      CLEAR: wa_head, lv_vbeln.

     endif.

    ENDLOOP.


  ENDMETHOD.


  METHOD if_apj_dt_exec_object~get_parameters.

  ENDMETHOD.


  METHOD if_oo_adt_classrun~main.

    createso(  ).

  ENDMETHOD.

ENDCLASS.
