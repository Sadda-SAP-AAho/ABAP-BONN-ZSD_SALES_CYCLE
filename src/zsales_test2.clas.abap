CLASS zsales_test2 DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
  INTERFACES if_oo_adt_classrun.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZSALES_TEST2 IMPLEMENTATION.


  METHOD if_oo_adt_classrun~main.

*   DATA : lv_del  TYPE string,
*           lv_msg2 TYPE string,
*           lv_msg3 TYPE string.
*    DATA : lv_date TYPE sy-datum.
*    lv_date = sy-datum.
*
*    DATA : lv_item_cid   TYPE string.
**  *******************************SALES ORDER CODE BEGIN**********************************************
*
*    DATA lv_vbeln TYPE string.
*
*    SELECT * FROM zinv_mst as a
*    WHERE a~reference_doc IS INITIAL AND a~reference_doc_del is INITIAL
*    INTO TABLE @DATA(it_head).
*
*   Loop at it_head into data(wa_head).
*
*    select single c~businesspartner,c~BusinessPartnerIDByExtSystem,d~CustomerAccountGroup
*   from zinv_mst as a
*   left join I_BusinessPartner as c on a~impartycode = c~BusinessPartnerIDByExtSystem
*   left join I_Customer as d on c~BusinessPartner = d~Customer
*   WHERE a~imno = @wa_head-imno
*   INTO  @DATA(wa_data_party).
*
*    SELECT a~idqtybag, a~remarks, a~idcat, a~idid, a~idno, b~imno, a~idpartycode, a~idprdcode, a~idprdqty,a~idprdrate,
*    a~idtdiscamt,b~plant,a~idprdbatch
*    FROM zinvoicedatatab1 AS a
*    LEFT JOIN zinv_mst AS b ON a~idno = b~imno
*    WHERE a~idno = @wa_head-imno
*    INTO TABLE @DATA(it_data).
*
*
*   DATA: var_sales_org type string.
*    data : var_org_div type string.
*
*  var_sales_org = wa_head-comp_code(2) && '00'.
*
*  data : var_dist type string.
*  if wa_data_party-CustomerAccountGroup = 'Z004'.
*    var_dist = 'ST'.
*    var_org_div = 'ST'.
*  else.
*    var_dist = 'GT'.
*    var_org_div = 'B1'.
*  endif.
*
*
*  data : final_rate type p decimals 2.
* Loop at it_data into data(wa_rate).
* wa_rate-idprdrate = wa_rate-idprdrate - ( wa_rate-idtdiscamt / wa_rate-idprdqty ).
*  MODIFY it_data FROM wa_rate.
* ENDLOOP.
*
************************************************************************************************************
*
*     if wa_head-reference_doc is INITIAL.
*      MODIFY ENTITIES OF i_salesordertp
*            ENTITY salesorder
*            CREATE
*            FIELDS ( salesordertype
*                   salesorganization distributionchannel organizationdivision
*                     soldtoparty purchaseorderbycustomer CustomerPaymentTerms )
*            WITH VALUE #( ( %cid = 'H001'
*                            %data = VALUE #(      salesordertype = 'TA'
*                                                  salesorganization = var_sales_org
*                                                  distributionchannel = var_dist
*                                                  organizationdivision = var_org_div
*                                                  soldtoparty = |{ wa_data_party-BusinessPartner ALPHA = IN }|
*                                                  purchaseorderbycustomer = wa_head-remarks
*                                                  CustomerPaymentTerms = '0001'
*                                              ) ) )
*
*
*
*      CREATE BY \_item
*      FIELDS ( Product RequestedQuantity Plant YY1_Discount_amt_sd_SDI YY1_Discount_amt_sd_SDIC batch  )
*      WITH VALUE #( ( %cid_ref = 'H001'
*                      salesorder = space
*                      %target = VALUE #( FOR wa_data IN it_data INDEX INTO i (
*                        %cid =  |I{ i WIDTH = 3 ALIGN = RIGHT PAD = '0' }|
*                        product =  wa_data-idprdcode
*                        requestedquantity =  wa_data-idprdqty
*                        plant = wa_head-plant
*                        YY1_Discount_amt_sd_SDI = wa_data-idtdiscamt
*                        YY1_Discount_amt_sd_SDIC = 'INR'
*                        batch = wa_data-idprdbatch
*                       ) ) ) )
*
*
*ENTITY SalesOrderItem
*  CREATE BY \_itempricingelement
*  FIELDS ( conditiontype conditionrateamount conditioncurrency conditionquantity )
*  WITH VALUE #(
*    FOR wa_data1 IN it_data INDEX INTO j
*    (
*      %cid_ref = |I{ j WIDTH = 3 ALIGN = RIGHT PAD = '0' }|
*      salesorder = space
*      salesorderitem = space
*      %target = VALUE #(
*      ( %cid = |ITPRELM{ j }_01|
*          conditiontype = 'ZBNP'
*          conditionrateamount = wa_data1-idprdrate
*          conditioncurrency = 'INR'
*          conditionquantity = '1'
*        )
*      )
*    )
*  )
*
*MAPPED DATA(ls_mapped)
*FAILED DATA(ls_failed)
*REPORTED DATA(ls_reported).
*
*
*      COMMIT ENTITIES BEGIN
*       RESPONSE OF i_salesordertp
*       FAILED DATA(ls_save_failed)
*       REPORTED DATA(ls_save_reported).
*
******salesorder conversion********
*
*      DATA: ls_so_temp_key              TYPE STRUCTURE FOR KEY OF i_salesordertp.
*
*      CONVERT KEY OF i_salesordertp FROM ls_so_temp_key TO DATA(ls_so_final_key).
*
******salesordeitem conversion********
*
*      TYPES: BEGIN OF ty_salesorderitem_key,
*               salesorder     TYPE i_salesorderitemtp-salesorder,
*               salesorderitem TYPE i_salesorderitemtp-salesorderitem,
*             END OF ty_salesorderitem_key.
*
*      DATA: lt_so_item_temp_keys  TYPE TABLE OF ty_salesorderitem_key,
*            lt_so_item_final_keys TYPE TABLE OF ty_salesorderitem_key,
*            ls_so_item_temp_key   TYPE ty_salesorderitem_key,
*            ls_so_item_final_key  TYPE ty_salesorderitem_key.
*
*      LOOP AT ls_mapped-salesorderitem ASSIGNING FIELD-SYMBOL(<ls_mapped_item>).
*        MOVE-CORRESPONDING <ls_mapped_item> TO ls_so_item_temp_key.
*        APPEND ls_so_item_temp_key TO lt_so_item_temp_keys.
*      ENDLOOP.
*
*      LOOP AT lt_so_item_temp_keys INTO ls_so_item_temp_key.
*        CONVERT KEY OF i_salesorderitemtp FROM ls_so_item_temp_key TO ls_so_item_final_key.
*        APPEND ls_so_item_final_key TO lt_so_item_final_keys.
*      ENDLOOP.
*
*
******salesordeitempricing  conversion********
*
*      TYPES: BEGIN OF ty_salesorderitempricingel_key,
*               salesorder              TYPE i_salesorderitemprcgelmnttp-salesorder,
*               salesorderitem          TYPE i_salesorderitemprcgelmnttp-salesorderitem,
*               pricingprocedurestep    TYPE i_salesorderitemprcgelmnttp-pricingprocedurestep,
*               pricingprocedurecounter TYPE i_salesorderitemprcgelmnttp-pricingprocedurecounter,
*             END OF ty_salesorderitempricingel_key.
*
*      DATA: lt_so_temp_keys_price  TYPE TABLE OF ty_salesorderitempricingel_key,
*            lt_so_final_keys_price TYPE TABLE OF ty_salesorderitempricingel_key,
*            ls_so_temp_key_price   TYPE ty_salesorderitempricingel_key,
*            ls_so_final_key_price  TYPE ty_salesorderitempricingel_key.
*
*      LOOP AT ls_mapped-salesorderitempricingelement ASSIGNING FIELD-SYMBOL(<ls_mapped_price>).
*        ls_so_temp_key_price = VALUE ty_salesorderitempricingel_key( ).
*        MOVE-CORRESPONDING <ls_mapped_price> TO ls_so_temp_key_price.
*        APPEND ls_so_temp_key_price TO lt_so_temp_keys_price.
*      ENDLOOP.
*
*      LOOP AT lt_so_temp_keys_price INTO ls_so_temp_key_price.
*        CONVERT KEY OF I_SalesOrderItemPrcgElmntTP FROM ls_so_temp_key_price TO ls_so_final_key_price.
*        APPEND ls_so_final_key_price TO lt_so_final_keys_price.
*      ENDLOOP.
*
*      DATA lv_error TYPE string.
*
*      IF ls_save_failed IS INITIAL.
**        lv_vbeln = | Sales Order created with number { ls_so_final_key-salesorder } . | .
*        lv_vbeln = ls_so_final_key-salesorder .
*
*        wa_head-processed = 'X'.
*        wa_head-reference_doc = lv_vbeln.
*        wa_head-status = 'Sales Order Created'.
*        MODIFY zinv_mst FROM @wa_head.
*      ELSE.
*        CLEAR lv_error.
*        LOOP AT ls_save_reported-salesorder ASSIGNING FIELD-SYMBOL(<fs_error>).
*          lv_error = lv_error && | { <fs_error>-%msg->if_message~get_text( ) } |.
*        ENDLOOP.
*        lv_error = |{ ls_save_reported-salesorder[ 1 ]-%msg->if_message~get_text( ) }|.
*        wa_head-error_log = lv_error.
*        MODIFY zinv_mst FROM @wa_head.
*      ENDIF.
*      COMMIT ENTITIES END.
*      ENDIF.
*
*      IF lv_vbeln IS NOT INITIAL.
*
*      MODIFY ENTITIES OF i_outbounddeliverytp
*           ENTITY outbounddelivery
*           EXECUTE createdlvfromsalesdocument
*           FROM VALUE #(
*           ( %cid = 'DLV001'
*           %param = VALUE #(
*           %control = VALUE #(
*           shippingpoint = if_abap_behv=>mk-on
*           deliveryselectiondate = if_abap_behv=>mk-on
*           deliverydocumenttype = if_abap_behv=>mk-on )
*           shippingpoint = wa_head-plant
*           deliveryselectiondate = lv_date
*           deliverydocumenttype = 'LF'
*           _referencesddocumentitem = VALUE #(
*           ( %control = VALUE #(
*           referencesddocument = if_abap_behv=>mk-on
*           )
*           referencesddocument = lv_vbeln
*           ) ) ) ) )
*           MAPPED DATA(ls_mapped2)
*           REPORTED DATA(ls_reported_modify2)
*           FAILED DATA(ls_failed_modify2).
*
*      DATA: ls_temporary_key TYPE STRUCTURE FOR KEY OF i_outbounddeliverytp.
*
*      COMMIT ENTITIES BEGIN
*       RESPONSE OF i_outbounddeliverytp
*       FAILED DATA(ls_failed_save)
*       REPORTED DATA(ls_reported_save).
*      CONVERT KEY OF i_outbounddeliverytp FROM ls_temporary_key TO DATA(ls_final_key).
*
*      IF ls_failed_modify2 IS INITIAL.
*        lv_del =   ls_final_key-outbounddelivery  .
*        wa_head-reference_doc_del = lv_del.
*        wa_head-status = 'Delivery Created'.
*        MODIFY zinv_mst FROM @wa_head.
*      ELSE.
*        lv_msg2 = ls_reported_save-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).
*        lv_msg2 = ls_reported_modify2-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).
*
*      ENDIF.
*      COMMIT ENTITIES END.
*    ENDIF.
*    CLEAR: wa_head, lv_vbeln, lv_del.
*      endloop.
  ENDMETHOD.
ENDCLASS.
