<?php 
  $curl = curl_init();
  $message = urlencode($message);
  curl_setopt($curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
  curl_setopt($curl, CURLOPT_URL, "www.way2sms.com/api/v1/sendCampaign/?apikey=E2EHFLHEKO5RRSVV23O6B46YB0LY2C2B&secret=CEUD2Y1OI6FUFNIX&usetype=stage&senderid=8097210903&phone=9820664507&message=".$message);
  curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
  $result = curl_exec($curl); 
  curl_close($curl); 
?>