CLASS zbilling_new DEFINITION
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



CLASS ZBILLING_NEW IMPLEMENTATION.


  METHOD if_apj_dt_exec_object~get_parameters.

  ENDMETHOD.


METHOD if_apj_rt_exec_object~execute.

    DATA : lv_msg2 TYPE string,
           lv_msg3 TYPE string,
           lv_msg  TYPE string.


    TYPES: BEGIN OF ty_del,
             reference_doc_invoice TYPE zinv_mst-reference_doc_invoice,
             reference_doc_del      TYPE zinv_mst-reference_doc_del,
             reference_doc      TYPE zinv_mst-reference_doc,
             status                TYPE zinv_mst-status,
             comp_code             TYPE zinv_mst-comp_code,
             imfyear               TYPE zinv_mst-imfyear,
             imno                  TYPE zinv_mst-imno,
             imtype                TYPE zinv_mst-imtype,
             DeliveryDocument    type I_DeliveryDocument-DeliveryDocument,
             SalesOrganization    type I_DeliveryDocument-SalesOrganization,
           END OF ty_del.

    DATA: it_del TYPE TABLE OF ty_del.




**   *********************************Billing Code Start**********************************************
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


    IF check = '1'.
    SELECT FROM I_DeliveryDocument AS a
  left JOIN zinv_mst AS b ON a~DeliveryDocument = b~reference_doc_del
  INNER JOIN zinv_mst_filter AS c
    ON c~comp_code  = b~comp_code
    AND c~plant     = b~plant
    AND c~imfyear   = b~imfyear
    AND c~imtype    = b~imtype
    AND c~imno      = b~imno
  FIELDS
    b~reference_doc_invoice,
    b~status,
    b~comp_code,
    b~imfyear,
    b~imno,
    b~imtype,
    a~DeliveryDocument
  WHERE
    a~OverallGoodsMovementStatus = 'C'
    AND a~SalesOrganization IN ('BN00', 'CA00', 'BI00')
    AND a~OverallSDProcessStatus <> 'C'
    AND b~reference_doc_del IS NOT INITIAL
    AND b~reference_doc IS NOT INITIAL
    AND b~reference_doc_invoice IS INITIAL
*    AND b~impartycode = '12510'
  INTO CORRESPONDING FIELDS OF TABLE @it_del.
    ELSE.
      SELECT FROM I_DeliveryDocument AS a
     LEFT JOIN zinv_mst AS b ON a~DeliveryDocument = b~reference_doc_del
     FIELDS a~DeliveryDocument,a~SalesOrganization , b~reference_doc_invoice,b~status,b~comp_code,b~imfyear,b~imno,b~imtype
     WHERE a~OverallGoodsMovementStatus = 'C'
    AND a~SalesOrganization IN ('BN00', 'CA00', 'BI00') AND a~OverallSDProcessStatus NE 'C' AND b~reference_doc_del IS NOT INITIAL
     INTO CORRESPONDING FIELDS OF TABLE @it_del.
    ENDIF.



    DATA: wa_del LIKE LINE OF it_del.

    IF it_del IS NOT INITIAL.
      LOOP AT it_del INTO wa_del.
        IF wa_del IS NOT INITIAL.
          MODIFY ENTITIES OF i_billingdocumenttp
          ENTITY billingdocument
          EXECUTE createfromsddocument AUTO FILL CID
          WITH VALUE #(
          ( %param = VALUE #( _reference = VALUE #( (
          sddocument =  wa_del-deliverydocument
          %control = VALUE #( sddocument = if_abap_behv=>mk-on ) ) )
          %control = VALUE #( _reference = if_abap_behv=>mk-on ) ) ) )

          RESULT DATA(lt_result_modify)
          FAILED DATA(ls_failed_modify)
          REPORTED DATA(ls_reported_modify).

          COMMIT ENTITIES BEGIN
          RESPONSE OF i_billingdocumenttp
          FAILED DATA(ls_failed_commit)
          REPORTED DATA(ls_reported_commit).

          CONVERT KEY OF i_billingdocumenttp FROM lt_result_modify[ 1 ]-%param-%pid TO DATA(ls_billingdocument).

          IF ls_failed_modify IS INITIAL.
            lv_msg3 =  ls_billingdocument-billingdocument .
*        wa_del-reference_doc_invoice = lv_msg3.
*        wa_del-status = 'Invoiced'.
*        modify zinv_mst from @wa_del.
            UPDATE zinv_mst SET
             reference_doc_invoice = @ls_billingdocument-billingdocument,
             status = 'Invoiced'
           WHERE reference_doc_del = @wa_del-deliverydocument.
          ELSE.
            lv_msg2 = | { ls_reported_commit-billingdocument[ 1 ]-%msg->if_message~get_longtext( ) } | .
            lv_msg =   ls_reported_modify-billingdocument[ 1 ]-%msg->if_message~get_text(  ) .
          ENDIF.

          COMMIT ENTITIES END.
        ENDIF.
        CLEAR : wa_del, lv_msg3,lv_msg,lv_msg2.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.


  METHOD if_oo_adt_classrun~main.


   DATA : lv_msg2 TYPE string,
           lv_msg3 TYPE string,
           lv_msg  TYPE string.


    TYPES: BEGIN OF ty_del,
             reference_doc_invoice TYPE zinv_mst-reference_doc_invoice,
             reference_doc_del      TYPE zinv_mst-reference_doc_del,
             reference_doc      TYPE zinv_mst-reference_doc,
             status                TYPE zinv_mst-status,
             comp_code             TYPE zinv_mst-comp_code,
             imfyear               TYPE zinv_mst-imfyear,
             imno                  TYPE zinv_mst-imno,
             imtype                TYPE zinv_mst-imtype,
             DeliveryDocument    type I_DeliveryDocument-DeliveryDocument,
             SalesOrganization    type I_DeliveryDocument-SalesOrganization,
           END OF ty_del.

    DATA: it_del TYPE TABLE OF ty_del.




**   *********************************Billing Code Start**********************************************
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


    IF check = '1'.
    SELECT FROM I_DeliveryDocument AS a
  left JOIN zinv_mst AS b ON a~DeliveryDocument = b~reference_doc_del
  INNER JOIN zinv_mst_filter AS c
    ON c~comp_code  = b~comp_code
    AND c~plant     = b~plant
    AND c~imfyear   = b~imfyear
    AND c~imtype    = b~imtype
    AND c~imno      = b~imno
  FIELDS
    b~reference_doc_invoice,
    b~status,
    b~comp_code,
    b~imfyear,
    b~imno,
    b~imtype,
    a~DeliveryDocument
  WHERE
    a~OverallGoodsMovementStatus = 'C'
    AND a~SalesOrganization IN ('BN00', 'CA00', 'BI00')
    AND a~OverallSDProcessStatus <> 'C'
    AND b~reference_doc_del IS NOT INITIAL
    AND b~reference_doc IS NOT INITIAL
    AND b~reference_doc_invoice IS INITIAL
*    AND b~impartycode = '12510'
  INTO CORRESPONDING FIELDS OF TABLE @it_del.
    ELSE.
      SELECT FROM I_DeliveryDocument AS a
     LEFT JOIN zinv_mst AS b ON a~DeliveryDocument = b~reference_doc_del
     FIELDS a~DeliveryDocument,a~SalesOrganization , b~reference_doc_invoice,b~status,b~comp_code,b~imfyear,b~imno,b~imtype
     WHERE a~OverallGoodsMovementStatus = 'C'
    AND a~SalesOrganization IN ('BN00', 'CA00', 'BI00') AND a~OverallSDProcessStatus NE 'C' AND b~reference_doc_del IS NOT INITIAL
     INTO CORRESPONDING FIELDS OF TABLE @it_del.
    ENDIF.



    DATA: wa_del LIKE LINE OF it_del.

    IF it_del IS NOT INITIAL.
      LOOP AT it_del INTO wa_del.
        IF wa_del IS NOT INITIAL.
          MODIFY ENTITIES OF i_billingdocumenttp
          ENTITY billingdocument
          EXECUTE createfromsddocument AUTO FILL CID
          WITH VALUE #(
          ( %param = VALUE #( _reference = VALUE #( (
          sddocument =  wa_del-deliverydocument
          %control = VALUE #( sddocument = if_abap_behv=>mk-on ) ) )
          %control = VALUE #( _reference = if_abap_behv=>mk-on ) ) ) )

          RESULT DATA(lt_result_modify)
          FAILED DATA(ls_failed_modify)
          REPORTED DATA(ls_reported_modify).

          COMMIT ENTITIES BEGIN
          RESPONSE OF i_billingdocumenttp
          FAILED DATA(ls_failed_commit)
          REPORTED DATA(ls_reported_commit).

          CONVERT KEY OF i_billingdocumenttp FROM lt_result_modify[ 1 ]-%param-%pid TO DATA(ls_billingdocument).

          IF ls_failed_modify IS INITIAL.
            lv_msg3 =  ls_billingdocument-billingdocument .
*        wa_del-reference_doc_invoice = lv_msg3.
*        wa_del-status = 'Invoiced'.
*        modify zinv_mst from @wa_del.
            UPDATE zinv_mst SET
             reference_doc_invoice = @ls_billingdocument-billingdocument,
             status = 'Invoiced'
           WHERE reference_doc_del = @wa_del-deliverydocument.
          ELSE.
            lv_msg2 = | { ls_reported_commit-billingdocument[ 1 ]-%msg->if_message~get_longtext( ) } | .
            lv_msg =   ls_reported_modify-billingdocument[ 1 ]-%msg->if_message~get_text(  ) .
          ENDIF.

          COMMIT ENTITIES END.
        ENDIF.
        CLEAR : wa_del, lv_msg3,lv_msg,lv_msg2.
      ENDLOOP.
    ENDIF.


*   data : lv_msg2 type string,
*         lv_msg3 type string,
*         lv_msg type string.
*
*
***   *********************************Billing Code Start**********************************************
* SELECT FROM I_DeliveryDocument AS a
* left join zinv_mst as b on a~DeliveryDocument = b~reference_doc_del
*  FIELDS a~DeliveryDocument,a~SalesOrganization , b~reference_doc_invoice,b~status,b~comp_code,b~imfyear,b~imno,b~imtype
*  WHERE a~OverallGoodsMovementStatus = 'C'
*    AND a~SalesOrganization IN ('BN00', 'CA00', 'BI00') and a~OverallSDProcessStatus ne 'C' and b~reference_doc_del is not initial
*  INTO table @DATA(it_del).
*
*  DATA: wa_del LIKE LINE OF it_del.
*
*if it_del is not INITIAL.
* LOOP at it_del into wa_del.
*   if wa_del is not INITIAL.
*    MODIFY ENTITIES OF i_billingdocumenttp
*      ENTITY billingdocument
*      EXECUTE createfromsddocument AUTO FILL CID
*      WITH VALUE #(
*      ( %param = VALUE #( _reference = VALUE #( (
*      sddocument =  wa_del-DeliveryDocument
*      %control = VALUE #( sddocument = if_abap_behv=>mk-on ) ) )
*      %control = VALUE #( _reference = if_abap_behv=>mk-on ) ) ) )
*
*      RESULT DATA(lt_result_modify)
*      FAILED DATA(ls_failed_modify)
*      REPORTED DATA(ls_reported_modify).
*
*      COMMIT ENTITIES BEGIN
*       RESPONSE OF i_billingdocumenttp
*       FAILED DATA(ls_failed_commit)
*       REPORTED DATA(ls_reported_commit).
*
*      CONVERT KEY OF i_billingdocumenttp FROM lt_result_modify[ 1 ]-%param-%pid TO DATA(ls_billingdocument).
*
*      IF ls_failed_modify IS INITIAL.
*        lv_msg3 =  ls_billingdocument-billingdocument .
**        wa_del-reference_doc_invoice = lv_msg3.
**        wa_del-status = 'Invoiced'.
**        modify zinv_mst from @wa_del.
*         UPDATE zinv_mst SET
*          reference_doc_invoice = @ls_billingdocument-billingdocument,
*          status = 'Invoiced'
*        WHERE reference_doc_del = @wa_del-DeliveryDocument.
*      ELSE.
*        lv_msg2 = | { ls_reported_commit-billingdocument[ 1 ]-%msg->if_message~get_longtext( ) } | .
*        lv_msg =   ls_reported_modify-billingdocument[ 1 ]-%msg->if_message~get_text(  ) .
*      ENDIF.
*
*      COMMIT ENTITIES END.
*      endif.
*clear : wa_del, lv_msg3,lv_msg,lv_msg2.
*Endloop.
*endif.
  ENDMETHOD.
ENDCLASS.
