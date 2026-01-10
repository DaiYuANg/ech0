package ech0;

import ech0.instance.DI;


public class Application {
  void main() {
    DI.INSTANCE.get(Ech0.class).start();
  }
}


