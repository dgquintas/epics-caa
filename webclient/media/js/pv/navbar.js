$(document).ready( function() {

    $('select#mode-selector').change( function() {
        $('#monitor-params, #scan-params').hide();
        var modename = $(this).find('option:selected').attr('id');
        if( modename && modename !== '' ){
            $('#'+modename+'-params').show();
        }
    });

    $("button#subscription-submit").click( function() {
        var form = $("form#subscription-form");
        form.submit();
    });

});
