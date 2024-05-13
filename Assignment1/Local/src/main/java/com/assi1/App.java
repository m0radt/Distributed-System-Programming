package com.assi1;



public class App {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0){
            System.out.println("no arguments inserted");
            return;
        }
        
        if (args[0].equals("Worker"))
            Worker.main(new String[]{});

		else if (args[0].equals("Manager"))
            Manager.main(new String[]{});
        
        else if (args.length == 4) 
            Local.main(new String []{args[0], args[1], args[2], args[3]});

        else if (args.length == 3)
        	Local.main(new String []{args[0], args[1], args[2]});

        
        else
            System.out.println("there is somthing wrong with the arguments you inserted");
    }
}