CLASS zdelivery_cycle DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES if_apj_dt_exec_object .
    INTERFACES if_apj_rt_exec_object .
    INTERFACES if_oo_adt_classrun.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zdelivery_cycle IMPLEMENTATION.
  METHOD if_apj_rt_exec_object~execute.
   DATA : lv_del  TYPE string,
           lv_msg2 TYPE string,
           lv_msg3 TYPE string.
    DATA(lv_date) = CL_ABAP_CONTEXT_INFO=>GET_SYSTEM_DATE( ).

    DATA : check TYPE c LENGTH 1.

    SELECT FROM zintegration_tab AS a
    FIELDS a~intgmodule,a~intgpath
    where a~intgmodule is not INITIAL
    INTO TABLE @DATA(it_integration).

    LOOP AT it_integration INTO DATA(wa_integration).
      IF wa_integration-intgmodule = 'SALESFILTER' AND wa_integration-intgpath IS NOT INITIAL.
        check = '1'.
      ENDIF.
    ENDLOOP.

    DATA : it_head TYPE TABLE OF zinv_mst.

    IF check = '1'.
      SELECT a~*
        FROM zinv_mst AS a
        INNER JOIN zinv_mst_filter AS b
           ON a~comp_code  = b~comp_code
          AND a~plant      = b~plant
          AND a~imfyear    = b~imfyear
          AND a~imtype     = b~imtype
          AND a~imno       = b~imno
       WHERE a~reference_doc IS NOT INITIAL
         AND a~reference_doc_del IS INITIAL
*         AND a~impartycode = '12510'
        INTO TABLE @it_head.
    ELSE.
      SELECT *
        FROM zinv_mst AS a
       WHERE a~reference_doc IS NOT INITIAL
         AND a~reference_doc_del IS INITIAL
*         AND a~impartycode = '12510'
        INTO TABLE @it_head.
    ENDIF.


*    SELECT * FROM zinv_mst AS a
*     WHERE a~reference_doc IS NOT INITIAL AND a~reference_doc_del IS INITIAL AND a~impartycode = '12510'
*     INTO TABLE @DATA(it_head).

    LOOP AT it_head INTO DATA(wa_head).
      IF wa_head-reference_doc_del IS INITIAL.

        MODIFY ENTITIES OF i_outbounddeliverytp
             ENTITY outbounddelivery
             EXECUTE createdlvfromsalesdocument
             FROM VALUE #(
             ( %cid = 'DLV001'
             %param = VALUE #(
             %control = VALUE #(
             shippingpoint = if_abap_behv=>mk-on
             deliveryselectiondate = if_abap_behv=>mk-on
             deliverydocumenttype = if_abap_behv=>mk-on )
             shippingpoint = wa_head-plant
             deliveryselectiondate = lv_date
             deliverydocumenttype = 'LF'
             _referencesddocumentitem = VALUE #(
             ( %control = VALUE #(
             referencesddocument = if_abap_behv=>mk-on
             )
             referencesddocument = wa_head-reference_doc
             ) ) ) ) )
             MAPPED DATA(ls_mapped2)
             REPORTED DATA(ls_reported_modify2)
             FAILED DATA(ls_failed_modify2).

        DATA: ls_temporary_key TYPE i_outbounddeliverytp-OutboundDelivery.

        COMMIT ENTITIES BEGIN
         RESPONSE OF i_outbounddeliverytp
         FAILED DATA(ls_failed_save)
         REPORTED DATA(ls_reported_save).

        CONVERT KEY OF i_outbounddeliverytp FROM ls_temporary_key TO DATA(ls_final_key).

        IF ls_failed_modify2 IS INITIAL.
          lv_del =   ls_final_key-outbounddelivery  .
          wa_head-reference_doc_del = lv_del.
          wa_head-status = 'Delivery Created'.
          MODIFY zinv_mst FROM @wa_head.
        ELSE.
          lv_msg2 = ls_reported_save-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).
          lv_msg2 = ls_reported_modify2-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).

        ENDIF.
        COMMIT ENTITIES END.
      ENDIF.
      CLEAR: wa_head, lv_del.
    ENDLOOP.

  ENDMETHOD.

  METHOD if_apj_dt_exec_object~get_parameters.

  ENDMETHOD.

  METHOD if_oo_adt_classrun~main.
    DATA : lv_del  TYPE string,
           lv_msg2 TYPE string,
           lv_msg3 TYPE string.
    DATA(lv_date) = CL_ABAP_CONTEXT_INFO=>GET_SYSTEM_DATE( ).

    DATA : check TYPE c LENGTH 1.

    SELECT FROM zintegration_tab AS a
    FIELDS a~intgmodule,a~intgpath
    where a~intgmodule is not INITIAL
    INTO TABLE @DATA(it_integration).

    LOOP AT it_integration INTO DATA(wa_integration).
      IF wa_integration-intgmodule = 'SALESFILTER' AND wa_integration-intgpath IS NOT INITIAL.
        check = '1'.
      ENDIF.
    ENDLOOP.

    DATA : it_head TYPE TABLE OF zinv_mst.

    IF check = '1'.
      SELECT a~*
        FROM zinv_mst AS a
        INNER JOIN zinv_mst_filter AS b
           ON a~comp_code  = b~comp_code
          AND a~plant      = b~plant
          AND a~imfyear    = b~imfyear
          AND a~imtype     = b~imtype
          AND a~imno       = b~imno
       WHERE a~reference_doc IS NOT INITIAL
         AND a~reference_doc_del IS INITIAL
*         AND a~impartycode = '12510'
        INTO TABLE @it_head.
    ELSE.
      SELECT *
        FROM zinv_mst AS a
       WHERE a~reference_doc IS NOT INITIAL
         AND a~reference_doc_del IS INITIAL
*         AND a~impartycode = '12510'
        INTO TABLE @it_head.
    ENDIF.


*    SELECT * FROM zinv_mst AS a
*     WHERE a~reference_doc IS NOT INITIAL AND a~reference_doc_del IS INITIAL AND a~impartycode = '12510'
*     INTO TABLE @DATA(it_head).

    LOOP AT it_head INTO DATA(wa_head).
      IF wa_head-reference_doc_del IS INITIAL.

        MODIFY ENTITIES OF i_outbounddeliverytp
             ENTITY outbounddelivery
             EXECUTE createdlvfromsalesdocument
             FROM VALUE #(
             ( %cid = 'DLV001'
             %param = VALUE #(
             %control = VALUE #(
             shippingpoint = if_abap_behv=>mk-on
             deliveryselectiondate = if_abap_behv=>mk-on
             deliverydocumenttype = if_abap_behv=>mk-on )
             shippingpoint = wa_head-plant
             deliveryselectiondate = lv_date
             deliverydocumenttype = 'LF'
             _referencesddocumentitem = VALUE #(
             ( %control = VALUE #(
             referencesddocument = if_abap_behv=>mk-on
             )
             referencesddocument = wa_head-reference_doc
             ) ) ) ) )
             MAPPED DATA(ls_mapped2)
             REPORTED DATA(ls_reported_modify2)
             FAILED DATA(ls_failed_modify2).

        DATA: ls_temporary_key TYPE i_outbounddeliverytp-OutboundDelivery.

        COMMIT ENTITIES BEGIN
         RESPONSE OF i_outbounddeliverytp
         FAILED DATA(ls_failed_save)
         REPORTED DATA(ls_reported_save).

        CONVERT KEY OF i_outbounddeliverytp FROM ls_temporary_key TO DATA(ls_final_key).

        IF ls_failed_modify2 IS INITIAL.
          lv_del =   ls_final_key-outbounddelivery  .
          wa_head-reference_doc_del = lv_del.
          wa_head-status = 'Delivery Created'.
          MODIFY zinv_mst FROM @wa_head.
        ELSE.
          lv_msg2 = ls_reported_save-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).
          lv_msg2 = ls_reported_modify2-outbounddelivery[ 1 ]-%msg->if_message~get_longtext(  ).

        ENDIF.
        COMMIT ENTITIES END.
      ENDIF.
      CLEAR: wa_head, lv_del.
    ENDLOOP.

  ENDMETHOD.

ENDCLASS.
