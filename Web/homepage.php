<!DOCTYPE html>
<html>
<head>
	<title>Home</title>
		
	</script>
	<?php
	require_once("cdn.php");
	require_once("remote.php");
	require_once("remote1.php");
	?>
</head>
<body>
	<nav class="navbar navbar-inverse">
		<div class="container-fluid row">
			<div class="navbar-header col-sm-11">
				<a class="navbar-brand" href="">DASHBOARD</a>
				
			</div>
			<div class="col-sm-1">
			<ul class="nav navbar-nav" style="float: left">
				<li><a  class="btn btn-link btn-lg" style="color: white" href="logout.php">logout</a></li>
				
			</ul>
			</div>
		</div>
	</nav>
	<br>

	<div class=" container well" style="text-align: center; background-color: #c2cad0">
		<?php
				$stat= $conn->prepare('SELECT `flag_var`, `flag_key` FROM `Flag` limit 1;');
				$stat->execute();
				$stat->bind_result($flag_var,$flag_key);
				$stat->fetch();
				$stat->close();
				$stat= $conn->prepare('SELECT * FROM `Accident` ORDER BY `date_time` DESC  limit 1;');
				$stat->execute();
				$stat->bind_result($Accident_id,$camera_id, $pathimg, $date_time);
				$stat->fetch();
				$stat->close();
				if($flag_var==1){
					echo '
						<div id="myModal" class="modal fade" id = mymodal"  role="dialog">
						  <div class="modal-dialog modal-lg" style="height:300px" >

						    <!-- Modal content-->
						    <div class="modal-content" >
						      <div class="modal-header">
						        <h4 class="modal-title">
						        	<div class="alert alert-danger" style= "background-color: #c2cad0;color:black">
  										<strong> ALert! </strong> New Accident!

						  			<button type="button" style="float:right" class="btn btn-danger" data-dismiss="modal">X</button>
									</div>
						        </h4>
						      </div>
						      <div class="modal-body" style= "background-color: #c2cad0">
						        <img src="'.$pathimg.'"> <button class="btn btn-link">'.$date_time.'</button>
						        <button class="btn btn-defualt">'.$camera_id.'</button></button>
		    					<button class="btn btn-danger">Contact Emergency Services</button>
						      </div>
						      <div class="modal-footer">
						        
						      </div>
						    </div>

						  </div>
						</div>';








					
		    	
				}
				else {
					echo "<a  class='btn btn-default btn-lg' style='width: 900px;  background-color: #c2cad0'>
					No accident has happened <a href='homepage.php' class='btn btn-primary btn-lg' style='float: right'>REFRESH</a><h3>
					 ";
					
				}
				$stat = $conn->prepare('UPDATE `Flag` SET `flag_var`= 0 WHERE `flag_key` = 1;');
				$stat->execute();
				$stat->close();
		?>
	</div>

	<div class="row">
		<div class=" col-sm-1">
			
		</div>
		<div class="col-sm-4"  >
				<?php
				$stat= $conn->prepare('SELECT  `Address`, `Telephone` FROM `Monitoring_Region` WHERE `Region_id` = ?');
				$stat->bind_param('s', $_SESSION["user"]);
				$stat->execute();
				$stat->bind_result($Address, $Telephone);
				$stat->fetch();
				$stat->close();
		?>

				<div  class="well" style="overflow: scroll; text-align: center;background-color: #c2cad0;">
					<br>
					<img src='img/userlogo.png' style="border-radius: 120px; border: 5px solid black">
					<br><br>
					<div class="well" style="font-size: 20px"><?php echo $Address; ?></div>

					<div class="well"> <h4><?php echo $Telephone; ?></h4> </div>
					<div class="well" style="text-align: center; background-color: white">
						
						<?php 
						$stat= $conn -> prepare('SELECT `Name`, `Type`, `Address` , `Telephone` FROM `Emergency_Service` WHERE `Region_id` = ?');
							$stat->bind_param('s', $_SESSION["user"]);
							$stat->execute();
							$stat->bind_result($Name , $type, $Address, $Telephone);

				
							while($stat->fetch()){
								echo '
									<div class="well" style="text-align: center; background-color: #c2cad0;">
										<button class="btn btn-lg btn-defualt" >'.$Name.'</button><br>
										<button class="btn btn-link">'.$type.'</button>
										<span>'.$Address.'</span><br>
										<button class="btn btn-link" >'.$Telephone.'</button>
										<button class="btn btn-danger" >Contact</button>
										
									</div>
								';
							}
							$stat->close();
						?>
							
						<br>
						
						<br>
					</div>	
					<br>
					
				</div>
</div>
		<div class=" well container col-sm-6" style="background-color: #c2cad0;">
			<div class="alert alert-success" style="color: black; background-color: white">
  				<center><h1>DETECTED ACCIDENTS:</h1> </center>
			</div>
			 
		  	
		</div>
  		
	</div>


<script type="text/javascript">
    $(window).on('load',function(){
        $('#myModal').modal('show');
    });
</script>
</body>
</html>						

