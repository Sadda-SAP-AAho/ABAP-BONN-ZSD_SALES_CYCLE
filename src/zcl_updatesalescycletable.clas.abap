CLASS zcl_updatesalescycletable DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES if_oo_adt_classrun .
PROTECTED SECTION.
    data SalesOrder type string.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_updatesalescycletable IMPLEMENTATION.
    METHOD if_oo_adt_classrun~main.
    DATA : marksale TYPE int1.

    marksale = 0.
    SalesOrder = '0000000941'.

    IF marksale = 0.
      update  zinv_mst set reference_doc = '' , processed = '', reference_doc_del = '' where reference_doc = @SalesOrder and reference_doc is not INITIAL.
    ELSE.
     update  zinv_mst set reference_doc = 'X' , processed = 'X', reference_doc_del = 'X' where imno = '021728'.
     update  zinv_mst set reference_doc = 'X' , processed = 'X', reference_doc_del = 'X' where imno = '021741'.
     update  zinv_mst set reference_doc = 'X' , processed = 'X', reference_doc_del = 'X' where imno = '022554'.
   ENDIF.

   endMETHOD.
ENDCLASS.
