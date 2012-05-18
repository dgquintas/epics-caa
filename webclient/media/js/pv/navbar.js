function getCookie(name) {
    var r = document.cookie.match("\\b" + name + "=([^;]*)\\b");
    return r ? r[1] : undefined;
}

function unsubscribe(pvname){
    pvname = typeof pvname !== 'undefined' ? pvname : '';
    var url = '/subscriptions/' + pvname;
    var yesText = pvname ? "Yes, unsubscribe from <b>" + pvname + "</b>" : "Yes, unsubscribe from <b>all PVs</b>";

    bootbox.confirm('<h1>Are you sure?</h1>',
        "No, don't do anything",
        yesText,
        function(result){
            if (result) {
                $.ajax({
                    'url': url,
                    'type': 'DELETE',
                    'headers': {'X-XSRFToken': getCookie("_xsrf")}
                }).done( function(data) {
                    var box = bootbox.alert('<h1>Successfully unsubscribed</h1>');
                    console.log(JSON.stringify(data, null, 4));
                    setTimeout( function() {
                        box.modal('hide');
                        location.reload(true);
                    }, 2000 );
                }).fail( function(data) {
                    bootbox.dialog('<h1>Woops, something went wrong</h1><p><pre>'+ JSON.stringify(data, null, 4)+ '</pre></p>',
                        {'label': 'Ok, got it',
                         'class': 'btn-danger'});
                });
            }
        });
}



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

    $("button#config-submit").click( function() {
        var form = $("form#config-form");
        form.submit();
    });

    $('button#subscribe-button').click( function() {
        var pvname = $(this).attr('pvname');
        var modal = $('div#subscription_modal');
        $('input:text[name=pvname]',modal).val(pvname).prop('readonly', 'readonly');
        modal.modal('show');
    });


    $('button#unsubscribe-button').click( function() {
        var pvname = $(this).attr('pvname');
        unsubscribe(pvname);
    });

    $('a#unsubscribe-all').click( function(e) {
        e.preventDefault();
        unsubscribe();
    });

});
