function Checkform(el){
    var count = el.count.value;
    var summa = el.summa.value;
    var status = el.status.value;
    var ids = el.ids.value;
    var entry ={
        count: count,
        summa: summa,
        status: status,
        ids: ids
                }
        fetch('http://127.0.0.1:5000/result', {
            method: 'POST',
            credentials: 'include',
            cache: 'no-cache',
            body:  JSON.stringify({entry}),
            headers: new Headers ({
            'content-type': 'application/json'
                                })
                                            })
             .catch (function (error) {
                console.log('error', error)
                     })
return false;
}



